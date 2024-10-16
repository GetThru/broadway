defmodule Broadway.Topology.ProducerStage do
  @moduledoc false
  use GenStage

  alias Broadway.Message
  alias Broadway.Topology.RateLimiter
  alias Broadway.Utility

  require Logger

  @spec start_link(term, non_neg_integer, GenServer.options()) :: GenServer.on_start()
  def start_link(args, index, opts \\ []) do
    GenStage.start_link(__MODULE__, {args, index}, opts)
  end

  @spec push_messages(GenServer.server(), [Message.t()]) :: :ok
  def push_messages(producer, messages) do
    GenStage.call(producer, {__MODULE__, :push_messages, messages})
  end

  @spec drain(GenServer.server()) :: :ok
  def drain(producer) do
    GenStage.cast(producer, {__MODULE__, :prepare_for_draining})
    GenStage.async_info(producer, {__MODULE__, :cancel_consumers})
  end

  @impl true
  def init({args, index}) do
    {module, arg} = args[:module]
    transformer = args[:transformer]
    dispatcher = args[:dispatcher]
    rate_limiter = args[:rate_limiter]

    min_rate_limit = args[:broadway][:min_rate_limit]
    allowed_messages = get_in(args, [:broadway, :producer, :rate_limiting, :allowed_messages])

    if not is_nil(allowed_messages) and allowed_messages < min_rate_limit do
      name = args[:broadway][:name]

      error_message =
        "Minimum rate limit #{inspect(min_rate_limit)} too low for rate limiting allowed messages: #{inspect(allowed_messages)} given to #{inspect(name)}"

      Logger.debug(error_message)
    end

    # Inject the topology index only if the args are a keyword list.
    arg =
      if Keyword.keyword?(arg) do
        Keyword.put(arg, :broadway, Keyword.put(args[:broadway], :index, index))
      else
        arg
      end

    rate_limiting_state =
      if rate_limiter do
        rate_limiter_ref = RateLimiter.get_rate_limiter_ref(rate_limiter)

        %{
          state: :open,
          draining?: false,
          rate_limiter: rate_limiter_ref,
          # A queue of messages that we buffered.
          message_buffer: :queue.new(),
          # A queue of demands (integers) that we buffered.
          demand_buffer: :queue.new()
        }
      else
        nil
      end

    state = %{
      module: module,
      module_state: nil,
      transformer: transformer,
      consumers: [],
      rate_limiting: rate_limiting_state
    }

    case module.init(arg) do
      {:producer, module_state} ->
        {:producer, %{state | module_state: module_state}, dispatcher: dispatcher}

      {:producer, module_state, options} ->
        if options[:dispatcher] && options[:dispatcher] != dispatcher do
          raise "#{inspect(module)} is setting dispatcher to #{inspect(options[:dispatcher])}, " <>
                  "which is different from dispatcher #{inspect(dispatcher)} expected by Broadway"
        end

        {:producer, %{state | module_state: module_state}, [dispatcher: dispatcher] ++ options}

      return_value ->
        {:stop, {:bad_return_value, return_value}}
    end
  end

  @impl true
  def handle_subscribe(:consumer, _, from, state) do
    {:automatic, update_in(state.consumers, &[from | &1])}
  end

  @impl true
  def handle_cancel(_, from, state) do
    {:noreply, [], update_in(state.consumers, &List.delete(&1, from))}
  end

  # If we're rate limited, we store the demand in the buffer instead of forwarding it.
  # We'll forward it once the rate limit is lifted.
  @impl true
  def handle_demand(demand, %{rate_limiting: %{state: :closed}} = state) do
    state = update_in(state.rate_limiting.demand_buffer, &:queue.in(demand, &1))
    {:noreply, [], state}
  end

  def handle_demand(demand, state) do
    %{module: module, module_state: module_state} = state
    handle_no_reply(module.handle_demand(demand, module_state), state)
  end

  @impl true
  def handle_call({__MODULE__, :push_messages, messages}, _from, state) do
    {:reply, :ok, messages, state}
  end

  def handle_call(message, from, state) do
    %{module: module, module_state: module_state} = state

    message
    |> module.handle_call(from, module_state)
    |> case do
      {:reply, reply, events, new_module_state} ->
        messages = transform_events(events, state.transformer)
        {state, messages} = maybe_rate_limit_and_buffer_messages(state, messages)
        {:reply, reply, messages, %{state | module_state: new_module_state}}

      {:reply, reply, events, new_module_state, :hibernate} ->
        messages = transform_events(events, state.transformer)
        {state, messages} = maybe_rate_limit_and_buffer_messages(state, messages)
        {:reply, reply, messages, %{state | module_state: new_module_state}, :hibernate}

      {:stop, reason, reply, new_module_state} ->
        {:stop, reason, reply, %{state | module_state: new_module_state}}

      other ->
        handle_no_reply(other, state)
    end
  end

  @impl true
  def handle_cast({__MODULE__, :prepare_for_draining}, state) do
    %{module: module, module_state: module_state} = state

    if function_exported?(module, :prepare_for_draining, 1) do
      module_state
      |> module.prepare_for_draining()
      |> handle_no_reply(state)
    else
      {:noreply, [], state}
    end
  end

  def handle_cast(message, state) do
    %{module: module, module_state: module_state} = state

    message
    |> module.handle_cast(module_state)
    |> handle_no_reply(state)
  end

  @impl true
  def handle_info({__MODULE__, :cancel_consumers}, %{rate_limiting: %{} = rate_limiting} = state) do
    rate_limiting = %{rate_limiting | draining?: true}

    if :queue.is_empty(rate_limiting.message_buffer) do
      cancel_consumers(state)
    end

    {:noreply, [], %{state | rate_limiting: rate_limiting}}
  end

  def handle_info({__MODULE__, :cancel_consumers}, state) do
    cancel_consumers(state)
    {:noreply, [], state}
  end

  # Don't forward buffered demand when we're draining or when the rate limiting is closed.
  def handle_info(
        {__MODULE__, :handle_next_demand},
        %{rate_limiting: %{draining?: draining?, state: rl_state}} = state
      )
      when draining? or rl_state == :closed do
    {:noreply, [], state}
  end

  def handle_info({__MODULE__, :handle_next_demand}, state) do
    case get_and_update_in(state.rate_limiting.demand_buffer, &:queue.out/1) do
      {{:value, demand}, state} ->
        case handle_demand(demand, state) do
          {:noreply, messages, state} ->
            schedule_next_handle_demand_if_any(state)
            {:noreply, messages, state}

          {:noreply, messages, state, :hibernate} ->
            schedule_next_handle_demand_if_any(state)
            {:noreply, messages, state, :hibernate}

          {:stop, reason, state} ->
            {:stop, reason, state}
        end

      {:empty, state} ->
        {:noreply, [], state}
    end
  end

  # If the rate limit is lifted but our rate limiting state was "open",
  # we don't need to do anything since we don't have anything in the buffer.
  def handle_info({RateLimiter, :reset_rate_limiting}, %{rate_limiting: %{state: :open}} = state) do
    {:noreply, [], state}
  end

  def handle_info({RateLimiter, :reset_rate_limiting}, state) do
    state = put_in(state.rate_limiting.state, :open)

    {state, messages} = rate_limit_and_buffer_messages(state)

    # We'll schedule to handle the buffered demand regardless of
    # the state of rate limiting. We'll check if we can forward it
    # when handling the message.
    schedule_next_handle_demand_if_any(state)

    {:noreply, messages, state}
  end

  def handle_info(message, state) do
    %{module: module, module_state: module_state} = state

    message
    |> module.handle_info(module_state)
    |> handle_no_reply(state)
  end

  @impl true
  def terminate(reason, %{module: module, module_state: module_state}) do
    if function_exported?(module, :terminate, 2) do
      module.terminate(reason, module_state)
    else
      :ok
    end
  end

  defp handle_no_reply(reply, %{transformer: transformer} = state) do
    case reply do
      {:noreply, events, new_module_state} when is_list(events) ->
        messages = transform_events(events, transformer)
        {state, messages} = maybe_rate_limit_and_buffer_messages(state, messages)
        {:noreply, messages, %{state | module_state: new_module_state}}

      {:noreply, events, new_module_state, :hibernate} ->
        messages = transform_events(events, transformer)
        {state, messages} = maybe_rate_limit_and_buffer_messages(state, messages)
        {:noreply, messages, %{state | module_state: new_module_state}, :hibernate}

      {:stop, reason, new_module_state} ->
        {:stop, reason, %{state | module_state: new_module_state}}
    end
  end

  defp transform_events(events, nil) do
    case events do
      [] -> :ok
      [message | _] -> validate_message(message)
    end

    events
  end

  defp transform_events(events, {m, f, opts}) do
    for event <- events do
      message = apply(m, f, [event, opts])
      validate_message(message)
    end
  end

  defp validate_message(%Message{} = message) do
    message
  end

  defp validate_message(_message) do
    raise "the produced message is invalid. All messages must be a %Broadway.Message{} " <>
            "struct. In case you're using a standard GenStage producer, please set the " <>
            ":transformer option to transform produced events into message structs"
  end

  defp maybe_rate_limit_and_buffer_messages(state, messages) do
    if state.rate_limiting && messages != [] do
      state = update_in(state.rate_limiting.message_buffer, &enqueue_batch(&1, messages))
      rate_limit_and_buffer_messages(state)
    else
      {state, messages}
    end
  end

  defp rate_limit_and_buffer_messages(%{rate_limiting: %{state: :closed}} = state) do
    {state, []}
  end

  defp rate_limit_and_buffer_messages(%{rate_limiting: rate_limiting} = state) do
    %{message_buffer: buffer, rate_limiter: rate_limiter, draining?: draining?} = rate_limiting

    {rate_limiting, messages_to_emit} =
      case RateLimiter.get_currently_allowed(rate_limiter) do
        # No point in trying to emit messages if no messages are allowed. In that case,
        # we close the rate limiting and don't emit anything.
        allowed when allowed <= 0 ->
          Utility.maybe_log("Rate limiting closed", state)
          {%{rate_limiting | state: :closed}, []}

        allowed ->
          {allowed_left, probably_emittable, buffer, next_message_weight} =
            dequeue_many(buffer, allowed, [])

          log_map = %{
            initial_allowed: allowed,
            allowed_left: allowed_left,
            probably_emittable_count: length(probably_emittable),
            buffer_length: :queue.len(buffer),
            next_message_weight: next_message_weight
          }

          Utility.maybe_log("dequeue_many result: #{inspect(log_map)}", state)

          # If nothing was emittable, but the buffer has messages in it,
          # then we want to rate limit allowed_left. This will take the limit
          # down to 0.
          demand =
            case {length(probably_emittable), :queue.len(buffer)} do
              {0, buffer_length} when buffer_length > 0 -> allowed_left
              _ -> allowed - allowed_left
            end

          {rate_limiting_state, messages_to_emit, messages_to_buffer} =
            rate_limit_messages(
              rate_limiter,
              probably_emittable,
              demand,
              state
            )

          rate_limiting_map = %{
            rate_limiting_state: rate_limiting_state,
            emit_count: length(messages_to_emit),
            to_buffer_count: length(messages_to_buffer)
          }

          Utility.maybe_log("rate_limit_messages result: #{inspect(rate_limiting_map)}", state)

          new_buffer = enqueue_batch_r(buffer, messages_to_buffer)

          rate_limiting = %{
            rate_limiting
            | message_buffer: new_buffer,
              state: rate_limiting_state
          }

          if draining? and :queue.is_empty(new_buffer) do
            Utility.maybe_log("Cancelling consumers", state)
            cancel_consumers(state)
          end

          {rate_limiting, messages_to_emit}
      end

    {%{state | rate_limiting: rate_limiting}, messages_to_emit}
  end

  defp dequeue_many(queue, 0, acc), do: {0, Enum.reverse(acc), queue, 0}

  defp dequeue_many(queue, demand, acc) do
    case :queue.out(queue) do
      {{:value, message}, queue} ->
        if message.weight > demand do
          # requeue first message and ignore remaining demand since
          # the next message would put us over the allowed weight
          new_queue = :queue.in_r(message, queue)
          {demand, Enum.reverse(acc), new_queue, message.weight}
        else
          new_demand = demand - message.weight
          dequeue_many(queue, new_demand, [message | acc])
        end

      {:empty, queue} ->
        {demand, Enum.reverse(acc), queue, 0}
    end
  end

  defp enqueue_batch(queue, _list = []), do: queue

  defp enqueue_batch(queue, list) do
    :queue.join(queue, :queue.from_list(list))
  end

  defp enqueue_batch_r(queue, _list = []), do: queue

  defp enqueue_batch_r(queue, list) do
    :queue.join(:queue.from_list(list), queue)
  end

  defp rate_limit_messages(rate_limiter, messages, demand, state) do
    left = RateLimiter.rate_limit(rate_limiter, demand)
    Utility.maybe_log("Remaining rate limit: #{left}", state)

    case left do
      # If no more messages are allowed, we're rate limited but we're able
      # to emit all messages that we have.
      0 ->
        {:closed, messages, _to_buffer = []}

      # We were able to emit all messages and still more messages are allowed,
      # so the rate limiting is "open".
      left when left > 0 ->
        {:open, messages, _to_buffer = []}

      # We went over the rate limit, so we remove messages from the
      # back of the list of those we were able to emit until the
      # overflow is corrected and close the rate limiting.
      overflow when overflow < 0 ->
        reversed = Enum.reverse(messages)

        {emittable, to_buffer, _overflow} =
          Enum.reduce_while(reversed, {reversed, [], overflow}, fn
            message, {emittable, to_buffer, overflow} ->
              if overflow >= 0 do
                {:halt, {Enum.reverse(emittable), to_buffer, overflow}}
              else
                {:cont, {tl(emittable), [message | to_buffer], overflow + message.weight}}
              end
          end)

        {:closed, emittable, to_buffer}
    end
  end

  defp schedule_next_handle_demand_if_any(state) do
    if not :queue.is_empty(state.rate_limiting.demand_buffer) do
      send(self(), {__MODULE__, :handle_next_demand})
    end
  end

  defp cancel_consumers(state) do
    for from <- state.consumers do
      send(self(), {:"$gen_producer", from, {:cancel, :shutdown}})
    end
  end
end

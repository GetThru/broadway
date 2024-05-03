defmodule Broadway.Topology.RateLimiter do
  @moduledoc false

  use GenServer

  @atomics_index 1

  def start_link(opts) do
    case Keyword.fetch!(opts, :rate_limiting) do
      # If we don't have rate limiting options, we don't even need to start this rate
      # limiter process.
      nil ->
        :ignore

      rate_limiting_opts ->
        name = Keyword.fetch!(opts, :name)
        producers_names = Keyword.fetch!(opts, :producers_names)
        min_rate_limit = Keyword.fetch!(opts, :min_rate_limit)
        args = {rate_limiting_opts, producers_names, min_rate_limit}
        GenServer.start_link(__MODULE__, args, name: name)
    end
  end

  def rate_limit(counter, amount)
      when is_reference(counter) and is_integer(amount) and amount > 0 do
    :atomics.sub_get(counter, @atomics_index, amount)
  end

  def get_currently_allowed(counter) when is_reference(counter) do
    :atomics.get(counter, @atomics_index)
  end

  def update_rate_limiting(rate_limiter, opts) do
    GenServer.call(rate_limiter, {:update_rate_limiting, opts})
  end

  def get_rate_limiting(rate_limiter) do
    GenServer.call(rate_limiter, :get_rate_limiting)
  end

  def get_rate_limiter_ref(rate_limiter) do
    GenServer.call(rate_limiter, :get_rate_limiter_ref)
  end

  @impl true
  def init({rate_limiting_opts, producers_names, min_rate_limit}) do
    interval = Keyword.fetch!(rate_limiting_opts, :interval)
    allowed = Keyword.fetch!(rate_limiting_opts, :allowed_messages)

    {allowed, interval} = adjust_rate_limit_against_minimum(allowed, interval, min_rate_limit)

    counter = :atomics.new(@atomics_index, [])
    :atomics.put(counter, @atomics_index, allowed)

    timer = schedule_next_reset(interval)

    state = %{
      interval: interval,
      allowed: allowed,
      producers_names: producers_names,
      counter: counter,
      reset_timer: timer,
      min_rate_limit: min_rate_limit
    }

    {:ok, state}
  end

  @impl true
  def handle_call({:update_rate_limiting, opts}, _from, state) do
    %{
      interval: interval,
      allowed: allowed,
      reset_timer: prev_timer,
      min_rate_limit: min_rate_limit
    } = state

    new_allowed = Keyword.get(opts, :allowed_messages, allowed)
    new_interval = Keyword.get(opts, :interval, interval)

    {new_allowed, new_interval} =
      adjust_rate_limit_against_minimum(new_allowed, new_interval, min_rate_limit)

    state = %{state | interval: new_interval, allowed: new_allowed}

    if Keyword.get(opts, :reset, false) do
      cancel_reset_limit_timer(prev_timer)
      timer = schedule_next_reset(0)
      {:reply, :ok, %{state | reset_timer: timer}}
    else
      {:reply, :ok, state}
    end
  end

  def handle_call(:get_rate_limiting, _from, state) do
    %{interval: interval, allowed: allowed} = state
    {:reply, %{interval: interval, allowed_messages: allowed}, state}
  end

  def handle_call(:get_rate_limiter_ref, _from, %{counter: counter} = state) do
    {:reply, counter, state}
  end

  @impl true
  def handle_info(:reset_limit, state) do
    %{producers_names: producers_names, interval: interval, allowed: allowed, counter: counter} =
      state

    :atomics.put(counter, @atomics_index, allowed)

    for name <- producers_names,
        pid = GenServer.whereis(name),
        is_pid(pid),
        do: send(pid, {__MODULE__, :reset_rate_limiting})

    timer = schedule_next_reset(interval)

    {:noreply, %{state | reset_timer: timer}}
  end

  defp schedule_next_reset(interval) do
    Process.send_after(self(), :reset_limit, interval)
  end

  defp cancel_reset_limit_timer(timer) do
    case Process.cancel_timer(timer) do
      false ->
        receive do
          :reset_limit -> :ok
        after
          0 -> raise "unknown timer #{inspect(timer)}"
        end

      _ ->
        :ok
    end
  end

  defp adjust_rate_limit_against_minimum(allowed, interval, minimum) when minimum > allowed do
    ratio = allowed / minimum
    increased_interval = ceil(interval / ratio)
    {minimum, increased_interval}
  end

  defp adjust_rate_limit_against_minimum(allowed, interval, _minimum), do: {allowed, interval}
end

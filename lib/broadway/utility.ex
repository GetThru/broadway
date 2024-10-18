defmodule Broadway.Utility do
  @moduledoc """
  This entire module is temporary while working on the rate limit bug in the
  Broadway fork.

  Once we're comfortable with the fix, this module and all usages
  of it should be removed. This includes removing a bunch of code around those
  log statements to setup data for logging.
  """
  require Logger

  # ABOMINATION: This helps us only log information about specific
  # consumers by looking at the name of the queue. This leaks information
  # about our app into our Broadway fork.
  @queues [
    # "text.carrier.tmobile.tcr_campaign.1.sms.messages",
    # "text.carrier.us_cellular.tcr_campaign.1.sms.messages",
    "text.provider.bandwidth.sms.messages"
  ]

  def maybe_log(message, %{context: %{config: %{queue: queue}}})
      when queue in @queues do
    Logger.debug("Queue: #{queue} #{message}")
  end

  def maybe_log(message, %{module_state: %{config: %{queue: queue}}})
      when queue in @queues do
    Logger.debug("Queue: #{queue} #{message}")
  end

  def maybe_log(_message, _state), do: :ok
end

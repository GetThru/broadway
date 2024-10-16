defmodule Broadway.Utility do
  require Logger

  @queues [
    "text.carrier.tmobile.tcr_campaign.1.sms.messages"
    # "text.provider.bandwidth.sms.messages"
  ]

  def maybe_log(message, %{context: %{config: %{queue: queue}}})
      when queue in @queues do
    Logger.info("Queue: #{queue} #{message}")
  end

  def maybe_log(message, %{module_state: %{config: %{queue: queue}}})
      when queue in @queues do
    Logger.info("Queue: #{queue} #{message}")
  end

  def maybe_log(_message, _state), do: :ok
end

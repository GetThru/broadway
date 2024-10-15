defmodule Broadway.Utility do
  require Logger

  def maybe_log(message, %{context: %{config: %{queue: queue}}})
      when queue in [
             "text.carrier.tmobile.tcr_campaign.1.sms.messages",
             "text.provider.bandwidth.sms.messages"
           ] do
    Logger.info("Queue: #{queue} #{message}")
  end

  def maybe_log(_message, _state), do: :ok
end

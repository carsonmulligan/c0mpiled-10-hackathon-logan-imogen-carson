class MessagesController < ApplicationController
  before_action :set_investigation

  def create
    user_text = params.require(:message).permit(:content).fetch(:content).to_s.strip
    return redirect_to(investigation_path(@investigation, tab: "chat")) if user_text.empty?

    @investigation.messages.create!(role: "user", content: user_text)
    @investigation.messages.create!(role: "assistant", content: stub_reply(user_text))
    redirect_to investigation_path(@investigation, tab: "chat")
  end

  private

  def set_investigation
    @investigation = Investigation.find(params[:investigation_id])
  end

  def stub_reply(prompt)
    key_present = ENV["CRUSTDATA_API_KEY"].present?
    enrichment = key_present ? "Crustdata key detected — would enrich here." : "No CRUSTDATA_API_KEY in env."
    "#{enrichment} You asked: \"#{prompt}\". (Stub reply — no live calls yet.)"
  end
end

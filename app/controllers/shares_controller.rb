class SharesController < ApplicationController
  before_action :set_investigation

  def create
    share = @investigation.shares.new(share_params)

    if share.save
      flash[:share_notice] = "Holocron share queued for #{share.recipient_email}."
    else
      flash[:share_alert] = share.errors.full_messages.to_sentence
    end

    redirect_to redirect_target
  end

  def destroy
    @investigation.shares.find(params[:id]).destroy
    redirect_to redirect_target
  end

  private

  def set_investigation
    @investigation = Investigation.find(params[:investigation_id])
  end

  def share_params
    permitted = params.require(:share).permit(
      :office_id,
      :permission,
      :recipient_email,
      :recipient_role,
      :message,
      dataset_slugs: []
    )
    permitted[:dataset_slugs] = Array(permitted[:dataset_slugs]).reject(&:blank?)
    permitted
  end

  def redirect_target
    if params[:return_to] == "report"
      report_investigation_path(@investigation, anchor: "share")
    else
      investigation_path(@investigation, tab: "shares")
    end
  end
end

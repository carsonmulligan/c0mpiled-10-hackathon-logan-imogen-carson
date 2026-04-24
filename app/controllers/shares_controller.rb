class SharesController < ApplicationController
  before_action :set_investigation

  def create
    @investigation.shares.create!(share_params.with_defaults(permission: "view"))
    redirect_to investigation_path(@investigation, tab: "shares")
  rescue ActiveRecord::RecordInvalid
    redirect_to investigation_path(@investigation, tab: "shares")
  end

  def destroy
    @investigation.shares.find(params[:id]).destroy
    redirect_to investigation_path(@investigation, tab: "shares")
  end

  private

  def set_investigation
    @investigation = Investigation.find(params[:investigation_id])
  end

  def share_params
    params.require(:share).permit(:office_id, :permission)
  end
end

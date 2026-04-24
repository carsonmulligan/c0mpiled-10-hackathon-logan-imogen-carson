class SourcesController < ApplicationController
  before_action :set_investigation

  def create
    @investigation.sources.create!(source_params.with_defaults(kind: "note"))
    redirect_to investigation_path(@investigation, tab: "sources")
  end

  def destroy
    @investigation.sources.find(params[:id]).destroy
    redirect_to investigation_path(@investigation, tab: "sources")
  end

  private

  def set_investigation
    @investigation = Investigation.find(params[:investigation_id])
  end

  def source_params
    params.require(:source).permit(:kind, :title, :url, :body)
  end
end

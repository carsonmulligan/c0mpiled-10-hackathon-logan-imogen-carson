class ScrapersController < ApplicationController
  before_action :set_investigation

  def create
    @investigation.scrapers.create!(scraper_params.with_defaults(kind: "web", status: "idle"))
    redirect_to investigation_path(@investigation, tab: "scrapers")
  end

  def destroy
    @investigation.scrapers.find(params[:id]).destroy
    redirect_to investigation_path(@investigation, tab: "scrapers")
  end

  def run
    scraper = @investigation.scrapers.find(params[:id])
    scraper.update!(status: "running", last_run_at: Time.current)
    scraper.scrape_runs.create!(status: "completed", started_at: Time.current, finished_at: Time.current, output: { stub: "scraper run dispatched (no real fetch)" })
    scraper.update!(status: "completed")
    redirect_to investigation_path(@investigation, tab: "scrapers")
  end

  private

  def set_investigation
    @investigation = Investigation.find(params[:investigation_id])
  end

  def scraper_params
    params.require(:scraper).permit(:name, :kind, :target_url)
  end
end

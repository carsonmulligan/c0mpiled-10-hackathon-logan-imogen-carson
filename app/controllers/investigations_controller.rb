class InvestigationsController < ApplicationController
  before_action :set_investigation, only: [:show, :setup, :edit, :update, :destroy]
  before_action :set_module_options, only: [:new, :create]

  def index
    redirect_to investigation_path(Investigation.order(created_at: :desc).first || Investigation.create!(name: "Untitled investigation"))
  end

  def show
    @investigations = Investigation.order(updated_at: :desc)
    @offices = Office.order(:name)
    @tab = params[:tab].presence_in(%w[sources scrapers chat shares]) || "sources"
  end

  def new
    @investigation = Investigation.new
    @intake_objective = ""
    @intake_links = ""
    @intake_module = @module_options.first
  end

  def create
    @intake_objective = intake_params[:objective].to_s.strip
    @intake_links = intake_params[:links].to_s.strip
    @intake_module = intake_params[:module_name].presence || @module_options.first
    @investigation = Investigation.new(investigation_params)

    if @intake_objective.blank?
      @investigation.errors.add(:base, "Objective can't be blank")
      render :new, status: :unprocessable_entity
      return
    end

    Investigation.transaction do
      @investigation.name = derived_name(@intake_objective)
      @investigation.description = @intake_objective
      @investigation.save!

      create_link_sources!
      create_module_note!
    end

    redirect_to setup_investigation_path(@investigation)
  rescue ActiveRecord::RecordInvalid
    render :new, status: :unprocessable_entity
  end

  def setup
    @setup_steps = [
      { number: 1, title: "Parsing Objective", details: [] },
      {
        number: 2,
        title: "Ingesting Context",
        details: ["Analyzing documents", "Scraping website links"]
      },
      {
        number: 3,
        title: "Connecting to Module",
        details: ["Comparing selectors", "Matching context to existing leads", "Pulling entity profiles"]
      }
    ]
  end

  def edit; end

  def update
    @investigation.update!(investigation_params)
    redirect_to investigation_path(@investigation)
  end

  def destroy
    @investigation.destroy
    redirect_to root_path
  end

  private

  def set_investigation
    @investigation =
      if params[:id] == "current"
        Investigation.order(updated_at: :desc).first || Investigation.create!(name: "Untitled investigation")
      else
        Investigation.find(params[:id])
      end
  end

  def investigation_params
    params.fetch(:investigation, {}).permit(:name, :description)
  end

  def intake_params
    params.fetch(:intake, {}).permit(:objective, :links, :module_name, files: [])
  end

  def set_module_options
    @module_options = ["Fentanyl Module", "Trade Diversion Module", "Chemical Supplier Module"]
  end

  def derived_name(objective)
    objective.to_s.split(/[\n\.]/).map(&:strip).find(&:present?).to_s.first(80).presence || "Untitled investigation"
  end

  def create_link_sources!
    extracted_links.each do |link|
      @investigation.sources.create!(
        kind: "url",
        title: link,
        url: link,
        body: "Added during intake"
      )
    end
  end

  def create_module_note!
    return if @intake_module.blank?

    @investigation.sources.create!(
      kind: "note",
      title: "Selected module",
      body: @intake_module
    )
  end

  def extracted_links
    @extracted_links ||= @intake_links.split(/\r?\n|,/).filter_map do |value|
      candidate = value.strip
      next if candidate.blank?

      candidate
    end.uniq
  end
end

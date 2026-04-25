class InvestigationsController < ApplicationController
  before_action :set_investigation, only: [:show, :setup, :report, :edit, :update, :destroy]
  before_action :set_module_options, only: [:new, :create]

  def index
    redirect_to investigation_path(Investigation.order(created_at: :desc).first || Investigation.create!(name: "Untitled investigation"))
  end

  def show
    @investigations = Investigation.order(updated_at: :desc)
    @offices = Office.order(:name)
    @tab = params[:tab].presence_in(%w[entities selectors cases patterns]) || "entities"
    @demo = demo_dataset
  end

  def new
    @investigation = Investigation.new
    @intake_objective = "I'm tracking RC Kairos, a Mexico-based wholesale chemicals broker advertising fentanyl precursors on ChemNet — I need their network, selectors, and related cases."
    @intake_links = "https://www.chemnet.com/sell/RC-Kairos/"
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

  def report
    @demo = demo_dataset
    render layout: "report"
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

  def demo_dataset
    rc_kairos = Crustdata::Cache.identify("RC Kairos").first || {}
    {
      crustdata: rc_kairos,
      summary: [
        "Matched 6 entities from your context to companies in the Fentanyl Module — RC Kairos pinned as the lead entity.",
        "Identified 30 selectors relevant to your investigation, sourced primarily from ChemNet supplier listings.",
        "Identified 2 related, ongoing DEA cases that link to this investigation.",
        "Discovered 3 previously unidentified patterns of behavior across the RC Kairos / China-Mexico precursor corridor."
      ],
      entities: [
        { name: "RC Kairos", status: "matched", phone: "Not on file", email: "Not on file", personnel: "—",
          country: "MX", source: "chemnet", crustdata_id: rc_kairos["company_id"],
          headquarters: rc_kairos["headquarters"],
          industry: (rc_kairos["linkedin_industries"] || []).first,
          employee_count_range: rc_kairos["employee_count_range"],
          revenue_low: rc_kairos["estimated_revenue_lower_bound_usd"],
          revenue_high: rc_kairos["estimated_revenue_upper_bound_usd"],
          linkedin_url: rc_kairos["linkedin_profile_url"] },
        { name: "Anhui Rencheng Technology Co., Ltd.", status: "matched", phone: "+86 551 6262 8311", email: "sales@rencheng-tech.cn", personnel: "Wei Zhang", country: "CN", source: "tradeford" },
        { name: "Hebei Atun Imp. & Exp. Trading Co.",   status: "matched", phone: "+86 311 8888 4429", email: "info@atun-trade.com", personnel: "Lin Xu", country: "CN", source: "chemnet" },
        { name: "Wuhan Senwayer Century Chem.",         status: "matched", phone: "+86 27 5970 6112",  email: "sales03@senwayer.com", personnel: "Hai Liu", country: "CN", source: "tradeford" },
        { name: "Shijiazhuang Sdyano Fine Chem.",       status: "matched", phone: "+86 311 6669 5121", email: "ada@sdyano.com",      personnel: "Ada Chen", country: "CN", source: "chemnet" },
        { name: "Yuhao Trading Pvt. (HK) Ltd.",         status: "unmatched", phone: "—", email: "—", personnel: "—", country: "HK", source: "alibaba" }
      ],
      selectors: [
        { kind: "Company Name (RC Kairos)", last_updated: "04/26/26", other_fields: 12 },
        { kind: "LinkedIn URL",             last_updated: "04/26/26", other_fields: 9 },
        { kind: "Marketplace Listing",      last_updated: "04/26/26", other_fields: 11 },
        { kind: "Person Name",              last_updated: "04/26/26", other_fields: 10 },
        { kind: "Username",                 last_updated: "04/26/26", other_fields: 10 },
        { kind: "Phone Number (HOYAN ad)",  last_updated: "04/24/26", other_fields: 8 },
        { kind: "Email Address",            last_updated: "04/25/26", other_fields: 7 },
        { kind: "Wickr Handle",             last_updated: "04/24/26", other_fields: 6 }
      ],
      cases: [
        { name: "Operation Iron Lattice", office: "DEA", linked_selectors: 3, last_updated: "08/09/2025", contact: "John Doe" },
        { name: "Bluewater Diversion",    office: "DEA", linked_selectors: 2, last_updated: "02/20/2026", contact: "Jane Fawn" }
      ],
      patterns: [
        { description: "China → Mexico precursor relay (RC Kairos)", first_seen: "10/07/2025", linked_selectors: 10 },
        { description: "ChemNet ad-pivot to encrypted handles",      first_seen: "1/20/2026",  linked_selectors: 15 },
        { description: "Coordinated CAS-number listing across HOYAN-tagged sellers", first_seen: "11/28/2025", linked_selectors: 8 }
      ]
    }
  end

  def extracted_links
    @extracted_links ||= @intake_links.split(/\r?\n|,/).filter_map do |value|
      candidate = value.strip
      next if candidate.blank?

      candidate
    end.uniq
  end
end

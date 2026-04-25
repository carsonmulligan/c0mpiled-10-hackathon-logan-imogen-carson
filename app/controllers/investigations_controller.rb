class InvestigationsController < ApplicationController
  before_action :set_investigation, only: [:show, :setup, :report, :graph, :edit, :update, :destroy]
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

  def graph
    @graph = ontology_graph
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

  def ontology_graph
    {
      nodes: [
        { id: "rc_kairos",        label: "RC Kairos",                    group: "company",     title: "Mexico-based wholesale chemicals broker. Crustdata 27357628." },
        { id: "chemnet",          label: "ChemNet",                      group: "marketplace", title: "B2B chemicals marketplace where RC Kairos and HOYAN advertise." },
        { id: "hoyan",            label: "HOYAN",                        group: "brand",       title: "Brand / alias appearing on packaging photos. Linked to Wuhan-area sellers." },
        { id: "anhui_rencheng",   label: "Anhui Rencheng",               group: "supplier",    title: "Chinese supplier matched on tradeford listings." },
        { id: "hubei_norna",      label: "Hubei Norna",                  group: "supplier",    title: "Chinese supplier — repeat seller on ChemNet." },
        { id: "wuhan_senwayer",   label: "Wuhan Senwayer",               group: "supplier",    title: "Wuhan-area precursor supplier; ANPP listings." },
        { id: "shijiazhuang",     label: "Shijiazhuang Sdyano",          group: "supplier",    title: "Hebei-based fine chemical supplier." },
        { id: "yuhao_hk",         label: "Yuhao Trading (HK)",           group: "broker",      title: "Hong Kong forwarder. UNMATCHED — flagged for further collection." },
        { id: "boc_piperidone",   label: "1-Boc-4-Piperidone",           group: "chemical",    title: "CAS 79099-07-3. Direct precursor to ANPP." },
        { id: "anpp",             label: "ANPP",                         group: "chemical",    title: "4-Anilino-N-phenethylpiperidine. Immediate fentanyl precursor." },
        { id: "phenylamino",      label: "1-N-Boc-4-(Phenylamino)pip.",  group: "chemical",    title: "CAS 125541-22-2 — surfaced on ChemNet listing (Figure 1)." },
        { id: "garza",            label: "Garza García, MX",             group: "location",    title: "RC Kairos HQ — Nuevo León, Mexico." },
        { id: "wuhan_loc",        label: "Wuhan, CN",                    group: "location",    title: "Cluster of Hubei-area suppliers." },
        { id: "anhui_loc",        label: "Anhui, CN",                    group: "location",    title: "Anhui Rencheng base." },
        { id: "wickr_claire",     label: "Wickr: clairelee1",            group: "handle",      title: "Encrypted-messenger handle on the HOYAN packaging." },
        { id: "threema_zk8",      label: "Threema: ZK8METMF",            group: "handle",      title: "Encrypted-messenger handle on the HOYAN packaging." },
        { id: "phone_cn",         label: "+86 199 7215 5905",            group: "comm",        title: "WhatsApp number from HOYAN ad (CAS 19099-93-5)." },
        { id: "email_claire",     label: "claire@hbyingong.com",         group: "comm",        title: "Email selector from 1-Boc-4-Piperidone packaging." },
        { id: "email_cassiel",    label: "Cassiel@whhoyan.com",          group: "comm",        title: "Email selector from HOYAN ad surface." },
        { id: "case_iron_lattice",label: "Operation Iron Lattice",       group: "case",        title: "DEA-2025-04412 — active precursor supply-chain case." },
        { id: "case_bluewater",   label: "Bluewater Diversion",          group: "case",        title: "HSI-2025-00891 — Hong Kong forwarder disruption." },
        { id: "john_doe",         label: "John Doe",                     group: "person",      title: "Lead analyst, DEA Operation Iron Lattice." },
        { id: "jane_fawn",        label: "Jane Fawn",                    group: "person",      title: "Lead analyst, HSI Bluewater Diversion." }
      ],
      edges: [
        { id: "e1",  from: "rc_kairos",      to: "chemnet",         label: "advertises_on" },
        { id: "e2",  from: "rc_kairos",      to: "garza",           label: "hq_in" },
        { id: "e3",  from: "rc_kairos",      to: "anhui_rencheng",  label: "sources_from" },
        { id: "e4",  from: "rc_kairos",      to: "hubei_norna",     label: "sources_from" },
        { id: "e5",  from: "rc_kairos",      to: "wuhan_senwayer",  label: "sources_from" },
        { id: "e6",  from: "rc_kairos",      to: "yuhao_hk",        label: "ships_via" },
        { id: "e7",  from: "hoyan",          to: "chemnet",         label: "advertises_on" },
        { id: "e8",  from: "hoyan",          to: "boc_piperidone",  label: "lists_product" },
        { id: "e9",  from: "hoyan",          to: "phenylamino",     label: "lists_product" },
        { id: "e10", from: "hoyan",          to: "wickr_claire",    label: "uses_handle" },
        { id: "e11", from: "hoyan",          to: "threema_zk8",     label: "uses_handle" },
        { id: "e12", from: "hoyan",          to: "phone_cn",        label: "uses_phone" },
        { id: "e13", from: "hoyan",          to: "email_claire",    label: "uses_email" },
        { id: "e14", from: "hoyan",          to: "email_cassiel",   label: "uses_email" },
        { id: "e15", from: "anhui_rencheng", to: "boc_piperidone",  label: "supplies" },
        { id: "e16", from: "anhui_rencheng", to: "anhui_loc",       label: "hq_in" },
        { id: "e17", from: "wuhan_senwayer", to: "anpp",            label: "supplies" },
        { id: "e18", from: "wuhan_senwayer", to: "wuhan_loc",       label: "hq_in" },
        { id: "e19", from: "hubei_norna",    to: "wuhan_loc",       label: "hq_in" },
        { id: "e20", from: "shijiazhuang",   to: "phenylamino",     label: "supplies" },
        { id: "e21", from: "boc_piperidone", to: "anpp",            label: "precursor_of" },
        { id: "e22", from: "case_iron_lattice", to: "rc_kairos",    label: "tracks" },
        { id: "e23", from: "case_iron_lattice", to: "hoyan",        label: "tracks" },
        { id: "e24", from: "case_bluewater",   to: "yuhao_hk",      label: "tracks" },
        { id: "e25", from: "john_doe",       to: "case_iron_lattice", label: "leads" },
        { id: "e26", from: "jane_fawn",      to: "case_bluewater",  label: "leads" },
        { id: "e27", from: "yuhao_hk",       to: "shijiazhuang",    label: "forwards_for" },
        { id: "e28", from: "hoyan",          to: "wuhan_senwayer",  label: "co_brands_with" }
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

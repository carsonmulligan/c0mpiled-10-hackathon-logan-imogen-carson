class Share < ApplicationRecord
  belongs_to :investigation
  belongs_to :office, optional: true

  PERMISSIONS = %w[view comment edit].freeze

  SENSITIVITY_TIERS = { "PUBLIC" => 0, "FOUO" => 1, "LES" => 2 }.freeze

  ROLES = [
    { slug: "partner_agency", label: "Partner Agency",     ceiling: "LES",
      blurb: "Federal partner with full operational access (DEA, HSI, FBI, DOD)." },
    { slug: "liaison",        label: "Liaison",            ceiling: "FOUO",
      blurb: "Foreign / interagency liaison. Cannot receive Law Enforcement Sensitive material." },
    { slug: "spoke_readonly", label: "Read-only Spoke",    ceiling: "FOUO",
      blurb: "Holocron spoke account at a partner office. Read-only, FOUO ceiling." },
    { slug: "public_affairs", label: "Public Affairs",     ceiling: "PUBLIC",
      blurb: "Press / public-affairs use. Only PUBLIC bundles can be shared." }
  ].freeze

  DATASETS = [
    { slug: "executive_summary",   label: "Executive Summary",                         sensitivity: "PUBLIC", blurb: "High-level findings — safe to share externally." },
    { slug: "context_entities",    label: "Context Entities — RC Kairos cluster",      sensitivity: "FOUO",   blurb: "Six matched companies + UNMATCHED Yuhao Trading." },
    { slug: "selectors_redacted",  label: "Selectors (redacted)",                      sensitivity: "FOUO",   blurb: "30 selectors with PII / handles masked." },
    { slug: "selectors_full",      label: "Selectors (full)",                          sensitivity: "LES",    blurb: "Unredacted selectors including encrypted handles." },
    { slug: "patterns_behavior",   label: "Patterns of Behavior",                      sensitivity: "FOUO",   blurb: "3 behavioral patterns spanning 33 selectors." },
    { slug: "dea_overlap",         label: "DEA Case Overlap — Iron Lattice / Bluewater", sensitivity: "LES", blurb: "Linked selectors against DEA-2025-04412 and HSI-2025-00891." },
    { slug: "hoyan_osint",         label: "HOYAN OSINT Advertisement",                 sensitivity: "FOUO",   blurb: "Open-source ad imagery, CAS pairs, contact identifiers." },
    { slug: "encrypted_handles",   label: "Encrypted Handles — Wickr / Threema",       sensitivity: "LES",    blurb: "Wickr clairelee1, Threema ZK8METMF + linked sellers." },
    { slug: "crustdata",           label: "Crustdata Firmographics — RC Kairos",       sensitivity: "FOUO",   blurb: "Cached company_id 27357628 enrichment record." },
    { slug: "graph_snapshot",      label: "Network Graph Snapshot",                    sensitivity: "FOUO",   blurb: "JSON dump of the 23-node ontology + analyst annotations." }
  ].freeze

  validates :permission, inclusion: { in: PERMISSIONS }, allow_nil: true
  validates :recipient_email, format: { with: URI::MailTo::EMAIL_REGEXP }, allow_blank: true
  validate  :datasets_within_role_ceiling

  before_validation :default_shared_at

  def role_definition
    ROLES.find { |r| r[:slug] == recipient_role }
  end

  def dataset_definitions
    Array(dataset_slugs).filter_map { |s| DATASETS.find { |d| d[:slug] == s } }
  end

  def email_recipient?
    recipient_email.present?
  end

  private

  def default_shared_at
    self.shared_at ||= Time.current if email_recipient?
  end

  def datasets_within_role_ceiling
    return unless email_recipient?

    role = role_definition
    if role.nil?
      errors.add(:recipient_role, "must be one of #{ROLES.map { |r| r[:slug] }.join(', ')}")
      return
    end

    ceiling = SENSITIVITY_TIERS[role[:ceiling]] || 0
    above_ceiling = dataset_definitions.select { |d| (SENSITIVITY_TIERS[d[:sensitivity]] || 0) > ceiling }
    return if above_ceiling.empty?

    errors.add(:dataset_slugs, "include #{above_ceiling.map { |d| d[:label] }.join(', ')} which exceed #{role[:label]}'s #{role[:ceiling]} ceiling")
  end
end

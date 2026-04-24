offices = [
  { name: "Drug Enforcement Administration", code: "DEA" },
  { name: "Homeland Security Investigations", code: "HSI" },
  { name: "Federal Bureau of Investigation", code: "FBI" },
  { name: "Interpol",                          code: "INTERPOL" },
  { name: "UN Office on Drugs and Crime",      code: "UNODC" },
  { name: "State Department — INL",            code: "INL" }
].map { |attrs| Office.find_or_create_by!(code: attrs[:code]) { |o| o.name = attrs[:name] } }

inv1 = Investigation.find_or_create_by!(name: "Precursor flows — North America") do |i|
  i.description = "Open-source review of fentanyl precursor procurement and shipment patterns into NA."
end

inv2 = Investigation.find_or_create_by!(name: "Marketplace listing sweep — Q2") do |i|
  i.description = "Cross-marketplace scrape of advertised precursor chemicals."
end

inv3 = Investigation.find_or_create_by!(name: "Logistics partner debrief notes") do |i|
  i.description = "Notes and shared spreadsheets from major logistics providers."
end

if inv1.sources.empty?
  inv1.sources.create!(kind: "url",      title: "DEA — National Drug Threat Assessment",  url: "https://www.dea.gov/resources", body: "Annual public threat assessment.")
  inv1.sources.create!(kind: "dataset",  title: "INCB Red List (subset)",                  url: "https://www.incb.org/", body: "Internationally controlled precursors.")
  inv1.sources.create!(kind: "document", title: "Internal: known supplier cluster (demo)", body: "Suppliers A, B, C with overlapping shipping addresses.")
  inv1.sources.create!(kind: "note",     title: "Analyst note — anomaly",                  body: "Repeat shipments routed through neutral third country.")
end

if inv2.sources.empty?
  inv2.sources.create!(kind: "url",      title: "ChemNet listings — query: 4-ANPP",  url: "https://www.chemnet.com/", body: "12 advertised suppliers.")
  inv2.sources.create!(kind: "dataset",  title: "Marketplace dump — week 16",         body: "CSV pulled by scraper run.")
end

if inv1.scrapers.empty?
  inv1.scrapers.create!(name: "DEA seizure list mirror", kind: "web",         target_url: "https://www.dea.gov/press-releases", status: "idle")
  inv1.scrapers.create!(name: "ChemNet supplier sweep",  kind: "marketplace", target_url: "https://www.chemnet.com/search?q=precursor", status: "completed", last_run_at: 2.hours.ago)
  inv1.scrapers.create!(name: "Crustdata web search",    kind: "api",         target_url: "https://api.crustdata.com/web/search", status: "idle")
end

if inv2.scrapers.empty?
  inv2.scrapers.create!(name: "Marketplace bulk listing", kind: "marketplace", target_url: "https://example-market.com/listings", status: "idle")
end

if inv1.messages.empty?
  inv1.messages.create!(role: "user",      content: "What suppliers appear in both the DEA list and the marketplace sweep?")
  inv1.messages.create!(role: "assistant", content: "Stub: would cross-reference DEA seizure entities with marketplace supplier names. (No live calls yet.)")
end

dea  = offices.find { |o| o.code == "DEA" }
hsi  = offices.find { |o| o.code == "HSI" }
intl = offices.find { |o| o.code == "INTERPOL" }

Share.find_or_create_by!(investigation: inv1, office: dea)  { |s| s.permission = "edit" }
Share.find_or_create_by!(investigation: inv1, office: hsi)  { |s| s.permission = "view" }
Share.find_or_create_by!(investigation: inv2, office: intl) { |s| s.permission = "comment" }

puts "Seeded #{Investigation.count} investigations, #{Office.count} offices, #{Source.count} sources, #{Scraper.count} scrapers, #{Share.count} shares."

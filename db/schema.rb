# This file is auto-generated from the current state of the database. Instead
# of editing this file, please use the migrations feature of Active Record to
# incrementally modify your database, and then regenerate this schema definition.
#
# This file is the source Rails uses to define your schema when running `bin/rails
# db:schema:load`. When creating a new database, `bin/rails db:schema:load` tends to
# be faster and is potentially less error prone than running all of your
# migrations from scratch. Old migrations may fail to apply correctly if those
# migrations use external dependencies or application code.
#
# It's strongly recommended that you check this file into your version control system.

ActiveRecord::Schema[8.0].define(version: 2026_04_24_222054) do
  create_table "investigations", force: :cascade do |t|
    t.string "name"
    t.text "description"
    t.datetime "created_at", null: false
    t.datetime "updated_at", null: false
  end

  create_table "messages", force: :cascade do |t|
    t.integer "investigation_id", null: false
    t.string "role"
    t.text "content"
    t.datetime "created_at", null: false
    t.datetime "updated_at", null: false
    t.index ["investigation_id"], name: "index_messages_on_investigation_id"
  end

  create_table "offices", force: :cascade do |t|
    t.string "name"
    t.string "code"
    t.datetime "created_at", null: false
    t.datetime "updated_at", null: false
  end

  create_table "scrape_runs", force: :cascade do |t|
    t.integer "scraper_id", null: false
    t.string "status"
    t.datetime "started_at"
    t.datetime "finished_at"
    t.json "output"
    t.datetime "created_at", null: false
    t.datetime "updated_at", null: false
    t.index ["scraper_id"], name: "index_scrape_runs_on_scraper_id"
  end

  create_table "scrapers", force: :cascade do |t|
    t.integer "investigation_id", null: false
    t.string "name"
    t.string "kind"
    t.string "target_url"
    t.string "status"
    t.datetime "last_run_at"
    t.datetime "created_at", null: false
    t.datetime "updated_at", null: false
    t.index ["investigation_id"], name: "index_scrapers_on_investigation_id"
  end

  create_table "shares", force: :cascade do |t|
    t.integer "investigation_id", null: false
    t.integer "office_id", null: false
    t.string "permission"
    t.datetime "created_at", null: false
    t.datetime "updated_at", null: false
    t.index ["investigation_id"], name: "index_shares_on_investigation_id"
    t.index ["office_id"], name: "index_shares_on_office_id"
  end

  create_table "sources", force: :cascade do |t|
    t.integer "investigation_id", null: false
    t.string "kind"
    t.string "title"
    t.string "url"
    t.text "body"
    t.json "metadata"
    t.datetime "created_at", null: false
    t.datetime "updated_at", null: false
    t.index ["investigation_id"], name: "index_sources_on_investigation_id"
  end

  add_foreign_key "messages", "investigations"
  add_foreign_key "scrape_runs", "scrapers"
  add_foreign_key "scrapers", "investigations"
  add_foreign_key "shares", "investigations"
  add_foreign_key "shares", "offices"
  add_foreign_key "sources", "investigations"
end

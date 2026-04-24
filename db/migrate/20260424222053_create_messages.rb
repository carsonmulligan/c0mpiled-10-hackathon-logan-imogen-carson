class CreateMessages < ActiveRecord::Migration[8.0]
  def change
    create_table :messages do |t|
      t.references :investigation, null: false, foreign_key: true
      t.string :role
      t.text :content

      t.timestamps
    end
  end
end

class CreateInvestigations < ActiveRecord::Migration[8.0]
  def change
    create_table :investigations do |t|
      t.string :name
      t.text :description

      t.timestamps
    end
  end
end

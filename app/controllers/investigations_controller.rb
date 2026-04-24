class InvestigationsController < ApplicationController
  before_action :set_investigation, only: [:show, :edit, :update, :destroy]

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
  end

  def create
    @investigation = Investigation.create!(investigation_params.with_defaults(name: "Untitled investigation"))
    redirect_to investigation_path(@investigation)
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
end

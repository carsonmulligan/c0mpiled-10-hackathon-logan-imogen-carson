Rails.application.routes.draw do
  get "up" => "rails/health#show", as: :rails_health_check

  resources :investigations do
    resources :sources, only: [:create, :destroy]
    resources :scrapers, only: [:create, :destroy] do
      member do
        post :run
      end
    end
    resources :messages, only: [:create]
    resources :shares, only: [:create, :destroy]
  end

  root "investigations#show", defaults: { id: "current" }
end

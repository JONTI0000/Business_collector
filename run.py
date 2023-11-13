from business_collector import BusinessCollector
import http.client
import json
import pandas as pd
import os

search = {
    "retail_and_shopping":["fashion stores","clothings shops","Jewellery shops","baby care shops","grocery shops","toy shops","arts & craft","home need shops","book stores",
                           "car dealerships","motorbike dealerships","music instruments shops","game shops","sports shops","camping equipment shops",
                           "electronic shops","pet supplies store","gift shops","charity","trade shops"],
    "Beauty_and_spas":["salons","barber shops","cosmetic shops","tattoo & piercing parlors","nail studios","spas"],
    "Food_and_drink":["cafes & coffee","takeaways","resturants","pubs","bars","clubs","catering","grills","breweries & distileries"],
    "Home_improvement":["lawn & garden services","home improvement stores","interior designers","tradespeople"],
    "health_and_fitness":["medical centres","fitness centres","gyms","dental care centres","sport centre",'vision care centres'],
    "travel_and_tourism":["hotels","holiday homes","travel agents","holiday resorts"],
    "auto_and_transport":["vehicle repair centres","parking","vehicle service centres"],
    "personal_services":["photography services","laudary & dry cleaning","printing services","estate agents","pet care"]
}

key = "7e7266ba-8be3-4cbf-a868-3d9a27167920"
ll = "@51.36,-0.14670,14z"
catergory = "personal_services"
town_center = "carshalton"
search_term = "photography services"
count=1

collector = BusinessCollector(api_key=key,catergory=catergory,town_center=town_center,search_term=search_term,ll=ll,count=count)
collector.make_directories()
collector.make_files()
collector.create_search_phrase()
collector.scrape_data()
collector.create_dataframe(collector.append_to_lists())
collector.remove_duplicates_from_df()
collector.cleaning_df()
collector.appending_to_csv()
#collector.appending_to_master_csv() dont call when testing
collector.remove_duplicates_df()
#collector.remove_duplicates_master_csv() dont call when testing
collector.document_credits_used()
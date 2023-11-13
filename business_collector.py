import http.client
import json
import pandas as pd
import os
import csv


class BusinessCollector:
    def __init__(self,api_key,catergory,town_center:str,search_term:str,ll,count:int=3) -> None:
        self.keyword = None
        self.town_center = town_center
        self.search_term = search_term
        self.start = 0
        self.count = count
        self.ll=ll
        self.pages = []
        self.api_key = api_key
        self.df = None
        self.catergory = catergory
        self.path = os.getcwd()
        self.credits_used = 0
    
    def __str__(self):
        return self.catergory
    
    def __repr__(self):
        return self.catergory 
    
    def calculate_credits_used(self,credits):
        self.credits_used = self.credits_used + credits
        return self.credits_used
    
    def document_credits_used(self):
        path = os.path.join(self.path,"credits_used.csv")
        with open(path,'a', newline='') as csv_file:
                        csv_writer = csv.writer(csv_file)
                        csv_writer.writerow((self.credits_used,self.keyword))


    def make_directories(self):
        """create the directory for the town center if it doesnt exsist"""
        path = os.path.join(self.path,self.town_center)
        if  not os.path.exists(path):
            try:
                os.makedirs(path)
            except OSError as e:
                print(f"Error creating directory {path}: {e}")
        else:
            print(f"{path} does exsist")
    
    def make_files(self):
        """create the files for the town center directory"""
        path = os.path.join(self.path,self.town_center)
        catergories= ["retail_and_shopping",  
            "Beauty_and_spas",
            "Food_and_drink",
            "Home_improvement",
            "health_and_fitness",
            "travel_and_tourism",
            "auto_and_transport",
            "personal_services",
            "business_services",
            "things_to_do" ]
        if len(os.listdir(path))==0:
            try:
                for catergory in catergories:
                    csv_file_name=os.path.join(path,f"{catergory}_{self.town_center}.csv")
                    column_names = ("name",
                        "address",
                        "website",
                        "b_type",
                        "phone")

                    with open(csv_file_name, 'a', newline='') as csv_file:
                        csv_writer = csv.writer(csv_file)
                        csv_writer.writerow(column_names)
                print("files created")
            except OSError as e:
                print(f"Error creating files: {e}")
    
    def create_search_phrase(self):
        """creating the keyword and returning the keyword the format is as followed
        'search_term' in 'town center '"""
        self.keyword = "{} in {}".format(self.search_term,self.town_center)
        print("search phrase is",self.keyword)
        return self.keyword 
    
    def test_connection(self,conn):
        """_summary_
        raising error for the wrong status code
        Args:
            conn : takes the conn request to the API
        Raises:
            Exception: stating the connection status and the reason
        """
        if conn.status != 200:
            raise Exception (f"Inability to make connection status code:{conn.status} reason:{conn.reason}")


    def scrape_data(self):
        """
        inititalizing a connection to the api and scraping the data by using the start as 0
        which means starting from the 1st page and the count is the no of pages to go to where it 
        is set as an average of 4 pages
        """
        print("scraping..")
        for i in range(self.count):
            conn = http.client.HTTPSConnection("api.scrape-it.cloud")
            payload = json.dumps({
            	"country": "UK",
            	"domain": "com",
            	"start":self.start,
            	"keyword":self.keyword,
            	"ll": self.ll
            })
            headers = {
            'x-api-key': self.api_key,
            'Content-Type': 'application/json'
            }
            conn.request("POST", "/scrape/google/locals", payload, headers)
            res = conn.getresponse()
            self.test_connection(conn=res)
            data = res.read()
            json_data = json.loads(data.decode("utf-8")) 

            self.calculate_credits_used(5)

            self.pages.append(json_data)
            self.start+=20 
        print("done scraping")

    def append_to_lists(self):
        """
        extracting the data from the JSON file and appending to the list which will return a dictionary
        with all the lists of data
        """
        name=[]
        address=[]
        website=[]
        b_type=[]
        phone=[]

        for page in self.pages:
            businesses = page["scrapingResult"]["locals"]
            for business in businesses:
                if "title" in business:
                    name.append(business["title"])
                else:
                    name.append("undefined")
                if "address" in business:
                    address.append(business["address"])
                else:
                    address.append("undefined")

                if "type" in business:
                    b_type.append(business["type"])
                else:
                    b_type.append("undefined")

                if "website" in business:
                    website.append(business["website"])
                else:
                    website.append("undefined")   
                if "phone" in business:
                    phone.append(business["phone"])
                else:
                    phone.append("undefined")
        pd_data = {
                "name":name,
                "address":address,
                "website":website,
                "b_type":b_type,
                "phone":phone,  
            }
        print("appended to list..")
        return pd_data


    def create_dataframe(self,data:dict):
        """creating a pandas dataframe with the data retrieved from the appending_list function"""
        self.df = pd.DataFrame(data)
        print("Total businesses collected:{}".format(len(self.df)))
    
    def remove_duplicates_from_df(self):
        """removing duplicates from the dataframe just used as a precautionary way"""
        self.df.drop_duplicates()
        print("Total no of businesses that are non duplicates:",len(self.df))
    
    def cleaning_df(self):
        """Removing the duplicate entries of the gathered businesses with 
        reference to all the businesses collected in the master all business
         file"""
        # Create a master DataFrame (master_df) without duplicates
        file_path = os.path.join(self.path,"businesses.csv")
        master_df = pd.read_csv(file_path)
        
        # Merge the two DataFrames and get an indicator column
        merged = self.df.merge(master_df, on=["name","address","website","phone","b_type"], how='left', indicator=True)

        # Filter out the non-duplicate entries
        non_duplicates = merged[merged['_merge'] == 'left_only']
        duplicates= merged[merged['_merge'] == 'both']

        # Drop the _merge
        non_duplicates = non_duplicates.drop(columns=['_merge'])

        print("""
No of Non-duplicate entries:{}
No of duplicate entries:{}
        """.format(len(non_duplicates),len(duplicates)))

        self.df = non_duplicates
    
    def appending_to_csv(self):
        """appending data to the csv file"""
        file_path=os.path.join(self.path,self.town_center,f"{self.catergory}_{self.town_center}.csv")
        self.df.set_index("name",inplace=True)
        self.df.to_csv(file_path,mode="a",header=False)
        print(f"data added to {file_path}")

    def appending_to_master_csv(self):
        """adding data to the csv file"""
        file_path = os.path.join(self.path,"businesses.csv")
        self.df.to_csv(file_path,mode="a",header=False )
        print(f"data added to master file {file_path}")
    
    def remove_duplicates_df(self):
        """removing duplicates from the catergory file just to be safe"""
        file_path=os.path.join(self.path,self.town_center,f"{self.catergory}_{self.town_center}.csv")
        df_rd = pd.read_csv(file_path)
        df_rd.set_index("name",inplace=True)
        df_rd = df_rd.drop_duplicates()

        df_rd.to_csv(file_path,mode="w",header=True )
        print("removing duplicates from csv")

    def remove_duplicates_master_csv(self):
        """removing duplicates from the master file just to be safe"""
        filepath = os.path.join(self.path,"businesses.csv")
        df_master = pd.read_csv(filepath)
        df_master.set_index("name",inplace=True)
        df_master = df_master.drop_duplicates()
        df_master.to_csv(filepath,mode="w",header=True )
        print("removing duplicates from master csv")






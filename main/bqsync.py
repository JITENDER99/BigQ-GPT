import os
import google.generativeai as genai
from google.cloud import bigquery
from google.auth.transport.requests import Request
from google.oauth2 import service_account
import hashlib
import ast
from abc import ABC, abstractmethod
from google.cloud import bigquery
import json





class GeminisetUp():

  with open('config/gemsecrets.json') as gem_bq:
    gem = json.load(gem_bq)

  # print(self.gem.keys())
  GOOGLE_API_KEY=gem['GOOGLE_API_KEY']
  genai.configure(api_key=GOOGLE_API_KEY)
  model = genai.GenerativeModel('gemini-pro')



class BQSetup(GeminisetUp):

  def __init__(self,credentials_path):  # using the default one right now
    self.scopes = ['https://www.googleapis.com/auth/cloud-platform']
    self.credentials = service_account.Credentials.from_service_account_file(credentials_path, scopes=self.scopes)


  @abstractmethod
  def setup(self):
    pass


class Usersync(BQSetup):
  client = None
  project_id = None
  dataset = None
  table = None
  dataframe = None
  backupdataframe = None
  columns = None
  array = None
  series = None
  piiColumns = None

  def __init__(self, credentials_path="config/bqsecrets.json"): # providing default values as of now
    super().__init__(credentials_path)


  def table_preview(self,project_id,dataset,table):
    self.project_id = project_id
    self.dataset = dataset
    self.table = table
    self.client = bigquery.Client(credentials=self.credentials)


    query = f"""select * from `{self.project_id}.{self.dataset}.{self.table}` limit 100"""
    print("Below are the result for ", f""" {query} """)
    query_job =  self.client.query(query)
    self.dataframe = query_job.to_dataframe().head(10)
    self.columns  = self.dataframe.columns
    self.array =  self.dataframe.values
    # self.dict = self.dataframe.to_dict(orient='series')  # can be utilize afterwards
    print(self.dataframe)
    return self.dataframe


  def isPII(self):

    def detect_pii_columns(dataframe):

      pii_patterns = {
          # 'Name': r'\b[A-Za-z\'\-\s]+\b',
          # 'Phone Number': r'\b(?:\+?[1-9]\d{0,3}\s*[\-.]?\s*)?(?:\(\d{1,4}\)|\d{1,4})?\s*[\-.]?\s*(?:[1-9]\d{1,14}|\d{1,4}\s*(?:[\-.]?\s*[1-9]\d{1,14})?)\b',
          'Email Address': r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
          'Credit Card': r'\b(?:\d[ -]*?){13,16}\b',
          'Social Security Number': r'\b(?:\d{3}-?\d{2}-?\d{4})\b'}

      pii_columns = {}

      for column in dataframe.columns:
          for pii_type, pattern in pii_patterns.items():
              if dataframe[column].astype(str).str.contains(pattern).any():
                  pii_columns[column] = pii_type

      return pii_columns


    pii_columns = detect_pii_columns(self.dataframe)
    self.piiColumns = pii_columns.keys()
    return self.piiColumns


  def isPII_gem(self):
    print(self.columns)
    piiCol = self.model.generate_content(f"On the basis of given list of columns name i.e {self.columns}, kindly provide me only the names of the columms which can be considered as PII columns. Answer should be given in the list format of python").text

    self.piiColumns = ast.literal_eval(piiCol)
    print(self.piiColumns, type(self.piiColumns))



  def Pii_encrypt(self):
    self.backupdataframe = self.dataframe

    def sha256_hash(value):
      sha256 = hashlib.sha256()
      sha256.update(value.encode('utf-8'))
      return sha256.hexdigest()


    for col in self.piiColumns:
      self.dataframe[col].fillna('', inplace=True)
      self.dataframe[col] = self.dataframe[col].astype(str)
      self.dataframe[col] = self.dataframe[col].apply(sha256_hash)

    return self.dataframe


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

  GOOGLE_API_KEY=gem['GOOGLE_API_KEY']
  genai.configure(api_key=GOOGLE_API_KEY)
  model = genai.GenerativeModel('gemini-pro')


class Geminipro(GeminisetUp):
  __dict = None
  results = None
  qaBank = None
  dataframe = None
  description={"Columns":[], "Description":[], "Datatype":[]}




  def load_data(self,data):
    self.__dict = data.to_dict(orient = "list")
    self.dataframe = data
    print("data has been loaded")


  def Columns_description(self):
    for k,v in self.__dict.items():

      key_description = self.model.generate_content(f"On the basis of given key value pair which serves like a column of a dataset kindly provide me definition of the key on the basis of values it has. {{{k} : {v}}} ? ").text
      key_datatype = self.model.generate_content(f"On the basis of given key value pair tell me what should be the correct datatype of the value {{{k} : {v[0]}}}?").text
      print(k)
      print(key_datatype)
      print(key_description)
      print("--------------")
      self.description["Columns"].append(k)
      self.description["Description"].append(key_description)
      self.description["Datatype"].append(key_datatype)

    self.results = pd.DataFrame.from_dict(self.description)

    return self.results


  def table_description(self):
      key_dataDescription = self.model.generate_content(f"On the basis of given dictionary {self.__dict} which is a kind of table only, can you please tell me the short summary of the table??").text
      print(key_dataDescription)



  def extract_questions_and_answers(self,text):
    import re

    questions = re.findall(r'\d+\.\s(.*?)\?', text)
    answers = re.findall(r'\d+\.\s(.*?)\.', text)

    # import pdb; pdb.set_trace()
    return {"Ques":questions,"Ans":answers}


  def analysis(self):
    key_analysis = self.model.generate_content(f"On the basis of given table {self.dataframe}, kindly provide me the 5 analytical based general questions along with their answers with respect to the mentioned table. Note that  questions should in a separate list and answers should be in a separate list").text
    print("***.....Here we go.....***")
    print(key_analysis)
    self.qaBank= self.extract_questions_and_answers(key_analysis)
    return pd.DataFrame.from_dict(self.qaBank)




  def save_csv(self,name):
    self.results.to_csv(name+".csv")
    print("File is successfully saved in csv format")


  def isPiigem(self):
    key_analysis = self.model.generate_content(f"On the basis of given table {self.dataframe}, Can you suggest which of these columns can be considerd as PII columns if no column is there than just return None otherwise provide the answer in the list format").text
    print("***.....Here we go.....***")
    print(key_analysis)
    # self.qaBank= self.extract_questions_and_answers(key_analysis)


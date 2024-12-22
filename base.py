from datetime import datetime
import logging
import json
from pyapacheatlas.core.util import GuidTracker
from pyapacheatlas.auth import ServicePrincipalAuthentication
from pyapacheatlas.core.typedef import (AtlasAttributeDef, Cardinality, EntityTypeDef, ParentEndDef, ChildEndDef, RelationshipTypeDef)
from pyapacheatlas.core import (AtlasClassification, AtlasEntity, AtlasProcess,
                                PurviewClient)
import os
import copy
from pyapacheatlas.core.typedef import EntityTypeDef
from pyapacheatlas.core.collections import PurviewCollectionsClient
import hashlib
import requests
import json
import urllib3
from azure.identity  import ClientSecretCredential



client_id = "myclientid"
tenant_id = 'mytenantid' 
client_secret = "myclientsecret"

auth= ServicePrincipalAuthentication(tenant_id, client_id, client_secret)

purview_account_name="mypurviewaccount"
purview_client = PurviewClient(
    account_name=purview_account_name,
    authentication=auth)
guidTracker = GuidTracker(-1000)



def table_entity_uplaod(table_names,qualified_name, attributes,typeName):
    
    tabel_entity = AtlasEntity(
            name=table_names,
            qualified_name=qualified_name,
            attributes=attributes,
            typeName=typeName,
            guid=guidTracker.get_guid()
        )
    # batch_upload.append(tabel_entity)
    # print("table_entity_uplaod",batch_upload)
    return tabel_entity
    
def atlas_process(input_table, output_table, name, qualified_name_process, type_name):
    print("inside process name is :", name)
    process_entity = AtlasProcess(
    name=name,
    typeName=type_name,
    qualified_name=qualified_name_process,
    inputs=input_table,
    outputs=output_table,
    guid=guidTracker.get_guid()
    )
    return process_entity

batch_upload = []
workspace_url = "myworkspaceurl"
token = "mybearertoken"


headers= {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}
get_all_catalog = f"{workspace_url}/api/2.1/unity-catalog/catalogs"
res_schema = requests.get(get_all_catalog, headers=headers)
if res_schema.status_code == 200:
  catalog_data = res_schema.json()
  for i in catalog_data['catalog']:
    
  get_all_schema_api = f"{workspace_url}/api/2.1/unity-catalog/schemas?catalog_name=catalog_name"
  res_schema = requests.get(get_all_schema_api, headers=headers)
  if res_schema.status_code == 200:
      schema_data = res_schema.json()
      for i in schema_data['schemas']:
        schema_name_to_be_used = i['name']
        if schema_name_to_be_used == 'test':
          table_api = f"{workspace_url}/api/2.1/unity-catalog/tables?catalog_name=codaap_a&schema_name={schema_name_to_be_used}"
          res_table = requests.get(table_api, headers=headers)
          if res_table.status_code == 200:
  
              table_data = res_table.json()
           
              
              
              table_entity_output = None
              table_entity_input = None
  
              
              for i in table_data['tables']: #dinner
                  cnt+=1
                  table_names = i['name']             
                  column_names = i['columns']  
                  typeName_table="databricks_table"
                  typeName_column="databricks_table_column"
  
                  input_table = []
                  output_table = []
  
                  col_map = []
  
                  qualified_table_name_output  =  f"{i['catalog_name']}.{i['schema_name']}.{table_names}"
                  # qualified_column_name_output = f"databricks://catalogs/{i['catalog_name']}/schemas/{i['schema_name']}/tables/{table_names}/columns/{col_name}"
  
                  attributes_table ={"description": "Azure Databricks table","type": "MANAGED", 'data_source_format': 'DELTA'}
                  # attributes_column={"description": "Azure Databricks Column","type": j['type_name']}
                  ###creating source entity
                  table_entity_output = table_entity_uplaod(table_names,qualified_table_name_output, attributes_table,typeName_table)
                  batch_upload.append(table_entity_output)
  
                  output_table.append(table_entity_output)
  
                  table_entity_input = None
  
                  
                  #5bf398          
                  for j in column_names: #full_name
                      col_name= j['name']
                      qualified_column_name_output = f"databricks://catalogs/{i['catalog_name']}/schemas/{i['schema_name']}/tables/{table_names}/columns/{col_name}"
                      attributes_column={"description": "Azure Databricks Column","dataType": j['type_name']}
                      column_entity_output = table_entity_uplaod(col_name,qualified_column_name_output, attributes_column,typeName_column)
                      column_entity_output.addRelationship(
                      table = table_entity_output,
  
                      guid=guidTracker.get_guid()
                      )
                      batch_upload.append(column_entity_output)
  
                      api_url_column_lineage = f"{workspace_url}/api/2.0/lineage-tracking/column-lineage?table_name=codaap_a.{schema_name_to_be_used}.{table_names}&column_name={col_name}"
  
                      res_column_lineage = requests.get(api_url_column_lineage, headers=headers)
                      if res_column_lineage.status_code == 200:
                          
                          column_level_lineage = res_column_lineage.json()
                          print("table and column is ", table_names, col_name)
                          print("lineage for column is:", column_level_lineage )
                          if not column_level_lineage :
                              print("No lineage for column")
                              purview_client.collections.upload_entities(batch=batch_upload, collection='123456')
                              continue
                          else:
                              if "upstream_cols" in column_level_lineage:
                                  for table_column_info in column_level_lineage['upstream_cols']: #menu app main full_menu 3 times 
  
                                      source_column_name_to_be_added = table_column_info['name']  #app
                                      source_table_name_to_be_added = table_column_info['table_name']  #menu
  
                                      qualified_table_name_input  =  f"{i['catalog_name']}.{i['schema_name']}.{source_table_name_to_be_added}"
                                      qualified_column_name_input = f"databricks://catalogs/{i['catalog_name']}/schemas/{i['schema_name']}/tables/{source_table_name_to_be_added}/columns/{source_column_name_to_be_added}"
  
                                      qualified_table_name_output  =  f"{i['catalog_name']}.{i['schema_name']}.{table_names}"
                                      qualified_column_name_output = f"databricks://catalogs/{i['catalog_name']}/schemas/{i['schema_name']}/tables/{table_names}/columns/{col_name}"
  
                                      attributes_table ={"description": "Azure Databricks table","type": "MANAGED", 'data_source_format': 'DELTA'}
                                      attributes_column={"description": "Azure Databricks Column","type": j['type_name']}
                                      ###creating source entity
                                      table_entity_input = table_entity_uplaod(source_table_name_to_be_added,qualified_table_name_input, attributes_table,typeName_table)
  
                                      match_table = purview_client.get_entity(qualifiedName = qualified_table_name_input, typeName=typeName_table)
                                      column_entity_input = None
  
                                      
                                      column_entity_input = table_entity_uplaod(source_column_name_to_be_added,qualified_column_name_input, attributes_column,typeName_column)
                                      column_entity_input.addRelationship(
                                      table = table_entity_input,
                                      guid=guidTracker.get_guid()
                                      )
                                      batch_upload.append(table_entity_input)  #menu
                                      batch_upload.append(column_entity_input) #app
                                      # print("input and output table:", table_entity_input, column_entity_input)
  
  
                                      dataset = {
                                                  "DatasetMapping": {"Source":table_entity_input.qualifiedName, "Sink": table_entity_output.qualifiedName},
                                                  "ColumnMapping": [
                                                      {"Source": source_column_name_to_be_added, "Sink": col_name}
                                                  ],
                                              }
                                      
  
                                      col_map.append(dataset)
  
                                      if table_entity_input in input_table:
                                          pass
                                      else:
  
                                          input_table.append(table_entity_input)
                      print("input and output table:", input_table, output_table)
  
                      qualified_name_process = f"{source_table_name_to_be_added}_{table_names}"
                      name = f"{source_table_name_to_be_added}_{table_names}"
  
                      print("name:", name)
                      type_name = "databricks_notebook_task"
  
                      if input_table and output_table:
                         
                          atlas_process_entity = atlas_process(input_table, output_table, name, qualified_name_process, type_name)
                          batch_upload.append(atlas_process_entity)
                          
  
                          
  
                          process_added =  AtlasProcess(
                          name=f"abc/{source_table_name_to_be_added}_{table_names}",
                          typeName="databricks_process",
                          qualified_name=f"databricks://abc/{source_table_name_to_be_added}_{table_names}", 
                          attributes = {"description": "Azure Databricks process"},
                          inputs = input_table,
                          outputs = output_table,
                          guid=guidTracker.get_guid()
                          )
  
                          process_added.attributes.update({"columnMapping": json.dumps(col_map)})
                          # # process.append(process_added)
                          atlas_process_entity.addRelationship(relationshipType =  'databricks_runnable_processes',processes= [process_added], guid=guidTracker.get_guid())
                          batch_upload.append(process_added)
  
                          purview_client.collections.upload_entities(batch=batch_upload, collection='123456')
                      
                      else:
                          purview_client.collections.upload_entities(batch=batch_upload, collection='123456')

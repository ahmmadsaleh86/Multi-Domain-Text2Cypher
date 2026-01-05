import re
import json
import copy
import numpy as np
from neo4j import GraphDatabase



GRAMMATICAL_VALIDATION_ERROR_MESSAGE = "You need to Turn on grammatical validation!\n You can do that by call turnon_grammatical_validation(neo4j_url, username, password)\n You should enter a valid values for neo4j_url, username, and password to use grammatical validation feature."
SCHEMA_VALIDATION_ERROR_MESSAGE = "You need to Turn on schema validation!\n You can do that by call turnon_schema_validation(schema_path)\n You should enter a valid path of a json file contains the graph databased schema to use schema validation feature.\n You can check Readme file for the decription of the desired json file."

class CypherValidator:
  def __init__(self, grammatical_validation=False,
               schema_validation=False,
               neo4j_url="",
               username="",
               password="",
               schema_path="",
               ):
    self.grammatical_validation_status = grammatical_validation
    self.schema_validation_status = schema_validation
    if self.grammatical_validation_status:
        self.__prepare_grammatical_validation(neo4j_url, username, password)
    if self.schema_validation_status:
        self.__prepare_schema_validation(schema_path)
          
  def __check_grammatical_validation_parameters(self, neo4j_url, username, password):
    if neo4j_url=="" or username=="" or password=="":
      raise Exception(GRAMMATICAL_VALIDATION_ERROR_MESSAGE)
          
  def __prepare_grammatical_validation(self, neo4j_url, username, password):
    self.__check_grammatical_validation_parameters(neo4j_url, username, password)
    self.driver = GraphDatabase.driver(neo4j_url, auth=(username, password))
  
  def turnon_grammatical_validation(self, neo4j_url, username, password):
    self.grammatical_validation_status = True
    self.__prepare_grammatical_validation(neo4j_url, username, password)

  def __check_schema_validation_parameters(self, schema_path):
    if schema_path=="":
      raise Exception(GRAMMATICAL_VALIDATION_ERROR_MESSAGE)
      
  def __prepare_schema_validation(self, schema_path):
    self.__check_schema_validation_parameters(schema_path)
    self.__initialzie_variabe_dict()
    self.__initialzie_schema_dict(schema_path)
    self.INNER_ATT_PATTERN = "\{*\w*.*?\}*"
    self.NODE_DEF_PATTERN = "\s*\(\s*\w*\s*\:\s*\w*\s*"+self.INNER_ATT_PATTERN+"\s*\)\s*"
    self.NODE_PATTERN = "\s*\(\s*\w*\s*\:*\s*\w*\s*"+self.INNER_ATT_PATTERN+"\s*\)\s*"
    self.LEFT_EDGE_PATTERN = "<{0,1}\s*\-\s*"
    self.RIGHT_EDGE_PATTERN = "\-\s*>{0,1}"
    self.RELATION_PATTERN = "\s*\[\s*\w*\s*\:*\s*\w*\s*"+self.INNER_ATT_PATTERN+"\s*\]\s*"
    self.ATTRIBUTE_PATTERN = '[a-zA-Z_]\w*\.\w+'
    self.RETURN_STAT_PATTERN = "[rR][eE][tT][uU][rR][nN]"
    self.RETURN_VAR_PATTERN = "\s[\w\W]*$"
    self.DATATYPE_LIST = ["boolean", "float", "integer", "path", "string","date", "time", "datetime", "duration", "point"] 
    
  def turnon_schema_validation(self, schema_path):
    self.schema_validation_status = True
    self.__prepare_schema_validation(schema_path)

    
  def __query_db(self, tx, query):
    return tx.run(query)

  def __initialzie_variabe_dict(self):
    self.variables_dic = {}
    self.num_var = 0
    return

  def __initialzie_schema_dict(self, schema_path=''):
    self.schema_dict = {}
    if schema_path != "":
      schema = json.load(open(schema_path))
      for element in schema:
        if element['Label'] in self.schema_dict:
          if element['Type']=="Relation":
            self.schema_dict[element['Label']]["From_To"].add((element['From'], element['To']))
          else:
            raise Exception("THE SCHEMA CONTAINS REPEATED NODE LABEL")
        else:
          self.schema_dict[element['Label']] = copy.deepcopy(element)
          if element['Type']=="Relation":
            del self.schema_dict[element['Label']]["From"], self.schema_dict[element['Label']]["To"]
            self.schema_dict[element['Label']]["From_To"] = set()
            self.schema_dict[element['Label']]["From_To"].add((element['From'], element['To']))
    return

  def __create_node_variables_dict(self, query):
    matches = re.findall(self.NODE_DEF_PATTERN, query)
    for element in matches:
      txt = ''.join(element.split()).strip()[1:-1]
      colon_indx = txt.find(':')
      variable = txt[:colon_indx].strip()
      if variable=="":
        variable = 'node'+str(self.num_var)
        index = element.index(':')
        query = query.replace(element, element[:index]+variable+element[index:],1)
      self.variables_dic[variable] = {'type':"Node", 'label':"", "attributes":[], 'alias':""}
      txt = txt[colon_indx+1:]
      if '{' in txt:
        curle_index = txt.find('{')
        self.variables_dic[variable]['label'] = txt[:curle_index].strip()
        txt = txt[curle_index+1:]
        txt = txt.replace('}','')
        for pair in txt.split(','):
          self.variables_dic[variable]['attributes'].append(pair.split(':')[0].strip())
      else:
        self.variables_dic[variable]['label'] = txt.strip()
      self.num_var += 1
    return query

  def __relation_breakdown(self, element):
    left_node = re.search(self.NODE_PATTERN, element).group()
    left_node_index = element.find(left_node)
    txt = element[left_node_index+len(left_node):]
    right_node = re.search(self.NODE_PATTERN, txt).group()
    right_node_index = txt.find(right_node)
    txt = txt[:right_node_index]
    relation = re.search(self.RELATION_PATTERN, txt).group()
    relation_index = txt.find(relation)
    left_egdge, right_edge = txt[:relation_index], txt[relation_index+len(relation):]
    return left_node, left_egdge, relation, right_edge, right_node

  
  def __extract_relation_variable(self, query, relation):
    txt = ''.join(relation.split()).strip()[1:-1]
    if ':' in txt:
      colon_indx = txt.find(':')
      variable = txt[:colon_indx].strip()
      if variable=="":
        variable = 'relation'+str(self.num_var)
        index = relation.index(':')
        query = query.replace(relation, relation[:index]+variable+relation[index:],1)
      self.variables_dic[variable] = {'type':"Relation", 'label':"", "attributes":[], 'alias':""}
      txt = txt[colon_indx+1:]
      if '{' in txt:
        curle_index = txt.find('{')
        self.variables_dic[variable]['label'] = txt[:curle_index].strip()
        txt = txt[curle_index+1:]
        txt = txt.replace('}','')
        for pair in txt.split(','):
          self.variables_dic[variable]['attributes'].append(pair.split(':')[0].strip())
      else:
        self.variables_dic[variable]['label'] = txt.strip()
      self.num_var +=1
    else:
      variable = txt.strip()
      self.variables_dic[variable] = {'type':"Relation", 'label':"", "attributes":[], 'alias':""}
      self.num_var +=1
    return query, variable
  

  def __extract_node_variable(self, node):
    txt = ''.join(node.split()).strip()[1:-1]
    if ':' in txt:
      colon_indx = txt.find(':')
      variable = txt[:colon_indx].strip()
    else:
      variable = txt
    label = self.variables_dic[variable]['label']
    return variable, label

  def __extract_relation_info(self, query, element):
    left_node, left_egdge, relation, right_edge, right_node = self.__relation_breakdown(element)
    query, variable = self.__extract_relation_variable(query, relation)
    self.variables_dic[variable]['undirected'] = left_egdge=='-' and right_edge=='-'
    if left_egdge=='-':
      self.variables_dic[variable]['from_variable'], self.variables_dic[variable]['from_label'] = self.__extract_node_variable(left_node)
      self.variables_dic[variable]['to_variable'], self.variables_dic[variable]['to_label'] = self.__extract_node_variable(right_node)
    else:
      self.variables_dic[variable]['from_variable'], self.variables_dic[variable]['from_label'] = self.__extract_node_variable(right_node)
      self.variables_dic[variable]['to_variable'], self.variables_dic[variable]['to_label'] = self.__extract_node_variable(left_node)
    return query

  def __replace_special_chat(self, txt):
    special_character = ['(', ')', '[', ']', '{', '}', '-', ':', '.', '?', '|', '$', '^', '*', '+']
    new_txt = txt
    for ch in special_character:
      if ch in txt:
        new_txt = new_txt.replace(ch, "\\"+ch)
    return new_txt

  def __isolate_relations(self, query):
    pattern = self.LEFT_EDGE_PATTERN+self.RELATION_PATTERN+self.RIGHT_EDGE_PATTERN
    candidates = re.findall(pattern, query)
    matches = []
    exanp_search = query
    search = query
    for can in candidates:
      can_pattern = self.__replace_special_chat(can)
      relation = re.findall("\(.*?\)\s*"+can_pattern+"\s*\(.*?\)", search)
      if len(relation)==0:
        relation = re.findall("\(.*?\)\s*"+can_pattern+"\s*\(.*?\)", exanp_search)
        relation = relation[0]
        index = exanp_search.index(relation)
        search = exanp_search[index+len(relation):]
        index = exanp_search.index(can)
        exanp_search = exanp_search[index+len(can):]
      else:
        relation = relation[0]
        index = search.index(can)
        exanp_search = search[index+len(can):]
        index = search.index(relation)
        search = search[index+len(relation):]
      matches.append(relation)
    return matches

  def __create_relation_variables_dict(self, query):
    matches = self.__isolate_relations(query)
    for element in matches:
      query = self.__extract_relation_info(query, element)
    return query

  def __add_alias_variable(self, query):
    alias_keyword = ["as", "As", "aS", "AS"]
    txt = query.split()
    for i in range(1, len(txt)-1):
      if txt[i] in alias_keyword:
        before_name = txt[i-1]
        if '.' in before_name:
          continue
        after_name = txt[i+1]
        after_name = after_name.replace(',','') if ',' in after_name else after_name
        if before_name in self.variables_dic:
          self.variables_dic[after_name] = copy.deepcopy(self.variables_dic[before_name])
          self.variables_dic[before_name]['alias'] = after_name
          self.variables_dic[after_name]['alias'] = before_name
    return

  def __update_attribute(self, query):
    matches = re.findall(self.ATTRIBUTE_PATTERN, query)
    for element in matches:
      variable = element.split('.')[0].strip()
      att = element.split('.')[1].strip()
      ###
      if variable in self.variables_dic:
          self.variables_dic[variable]['attributes'].append(att)
      elif variable.lower() in self.DATATYPE_LIST:
          continue
      else:
          raise Exception("The Variabel:"+variable+" is not defined")

  def __schema_label_matching(self, variable):
    if self.variables_dic[variable]['label']!="":
      return self.variables_dic[variable]['label'] in self.schema_dict
    else:
      return True

  def __schema_type_matching(self, variable):
    label = self.variables_dic[variable]['label']
    if label.strip()!='':
      return self.variables_dic[variable]['type'].lower() == self.schema_dict[label]['Type'].lower()
    else:
      return True

  def __schema_relation_direction_matching(self, variable):
    label = self.variables_dic[variable]['label']
    if label.strip()!="":
      if self.variables_dic[variable]['type'] == 'Relation':
        if self.variables_dic[variable]['undirected']: 
          if ((self.variables_dic[variable]['from_label'],self.variables_dic[variable]['to_label']) not in self.schema_dict[label]['From_To']) and ((self.variables_dic[variable]['to_label'],self.variables_dic[variable]['from_label']) not in self.schema_dict[label]['From_To']):
            return False
        else:
          if (self.variables_dic[variable]['from_label'],self.variables_dic[variable]['to_label']) not in self.schema_dict[label]['From_To']:
            return False
    return True

  def __schema_attributes_matching(self, variable):
    if len(self.variables_dic[variable]['attributes'])>0:
      label = self.variables_dic[variable]['label']
      if label not in self.schema_dict:
        return False
      for att in self.variables_dic[variable]['attributes']:
        if att not in self.schema_dict[label]['Attributes']:
          return False
    return True

  def __string_masking(self, query):
    matches = re.findall("\'.*?\'", query)
    for mat in matches:
      query = query.replace(mat, "'#'")
    matches = re.findall("\".*?\"", query)
    for mat in matches:
      query = query.replace(mat, '\"#\"')
    return query

  def __fix_relation_direction(self, query, variable):
    change = False
    label = self.variables_dic[variable]['label']
    if label.strip()!="":
      if self.variables_dic[variable]['type'] == 'Relation':
        if not self.variables_dic[variable]['undirected']:
          if ((self.variables_dic[variable]['from_label'],self.variables_dic[variable]['to_label']) in self.schema_dict[label]['From_To']) or ((self.variables_dic[variable]['to_label'],self.variables_dic[variable]['from_label']) in self.schema_dict[label]['From_To']):
            self.variables_dic[variable]['undirected'] = True
            exact_relation_pattern = "\s*\[\s*"+variable+"\s*\:\s*"+self.variables_dic[variable]['label']+"\s*\{*\w*.*?\}*\s*\]\s*"
            relation_edges = re.findall(self.LEFT_EDGE_PATTERN+exact_relation_pattern+self.RIGHT_EDGE_PATTERN, query)[0]
            relation = re.findall(exact_relation_pattern, relation_edges)[0]
            query = query.replace(relation_edges, '-'+relation+'-')
            change = True
    return query, change


  def remove_duplicate_return_variable(self, query):
    if not self.schema_validation_status:
      raise Exception(SCHEMA_VALIDATION_ERROR_MESSAGE)
    masked_query = self.__string_masking(query)
    original_return_variables = re.findall(self.RETURN_STAT_PATTERN+self.RETURN_VAR_PATTERN, masked_query)[-1][len('return '):]
    return_variables_lst = []
    for variable in original_return_variables.split(','):
      if variable.strip() not in return_variables_lst:
        return_variables_lst.append(variable.strip())
    return_variables = ', '.join(return_variables_lst)
    query = query.replace(original_return_variables, return_variables)
    return query

  def include_all_variable(self, query):
    if not self.schema_validation_status:
      raise Exception(SCHEMA_VALIDATION_ERROR_MESSAGE)
    masked_query = self.__string_masking(query)
    original_return_variables = re.findall(self.RETURN_STAT_PATTERN+self.RETURN_VAR_PATTERN, masked_query)[-1][len('return '):]
    return_variables_lst = []
    # REmove duplicate
    for variable in original_return_variables.split(','):
      if variable.strip() not in return_variables_lst:
        return_variables_lst.append(variable.strip())
    # add missing variables
    for variable in self.variables_dic:
      if self.variables_dic[variable]['alias']!='':
        if (variable not in return_variables_lst) and (self.variables_dic[variable]['alias'] not in return_variables_lst):
          return_variables_lst.insert(0, variable)
      else:
        if (variable not in return_variables_lst):
          return_variables_lst.insert(0, variable)
    return_variables = ', '.join(return_variables_lst)
    # Inject the new variables to the original query
    query = query.replace(original_return_variables, return_variables)
    return query

  def count_relations(self):
    if not self.schema_validation_status:
      raise Exception(SCHEMA_VALIDATION_ERROR_MESSAGE)
    if len(self.variables_dic)==0:
      raise Exception("You need first to call schema_valid(query) to parse the query first before counting the number of relations!")
    count = 0
    for variable in self.variables_dic:
      if self.variables_dic[variable]['type'] == 'Relation':
        count += 1
    return count

  def grammatical_valid(self, query):
    if not self.grammatical_validation_status:
      raise Exception(GRAMMATICAL_VALIDATION_ERROR_MESSAGE)
    gramatical_correct = True
    with self.driver.session() as session:
      try:
        _ = session.execute_write(self.__query_db, query)
      except:
        gramatical_correct = False
    return gramatical_correct

  def schema_valid(self, query):
    if not self.schema_validation_status:
      raise Exception(SCHEMA_VALIDATION_ERROR_MESSAGE)
    schem_correct = True
    try:
      self.__initialzie_variabe_dict()
      query = self.__create_node_variables_dict(query)
      self.__add_alias_variable(query)
      query = self.__create_relation_variables_dict(query)
      self.__add_alias_variable(query)
      masked_query = self.__string_masking(query)
      self.__update_attribute(masked_query)
      for variable in self.variables_dic:
        if not (self.__schema_label_matching(variable) and self.__schema_type_matching(variable) and self.__schema_attributes_matching(variable)):
          schem_correct = False
          break
        if not self.__schema_relation_direction_matching(variable):
          query, change = self.__fix_relation_direction(query, variable)
          if not change:
            schem_correct = False
            break
    except:
      schem_correct = False
    return query, schem_correct

  def query_exact_match(self, query1, query2):
    pre_query1 = ' '.join(query1.split())
    pre_query2 = ' '.join(query2.split())
    return pre_query1==pre_query2
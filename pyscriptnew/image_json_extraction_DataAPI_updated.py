import os
import requests
import json
import re
import copy
import logging
from datetime import datetime
from flask import Flask,jsonify,request
from werkzeug.utils import secure_filename

LOGGER_FOLDER = './dataAPIlogger_files'
dttoday=datetime.today().strftime('%Y-%m-%d')
# dttoday=datetime.today().strftime("%Y-%m-%d"+"_"+"%H:%M:%S")
if not os.path.exists(LOGGER_FOLDER):
    os.mkdir(LOGGER_FOLDER)

# logFile = LOGGER_FOLDER + scriptName + "_" + dateTimeStamp + ".log"
logFile = os.path.join(LOGGER_FOLDER,'DataAPI_updated_log'+"_"+str(dttoday)+".log")
print ('logfile--',logFile)
# create formatter         '%(asctime)-15s %(levelname)-8s %(message)s'
formatter = '%(message)s'
logging.basicConfig(level=logging.INFO, filename=logFile, filemode="w",format=formatter)
# #Creating an object 
logger=logging.getLogger() 
logging.info(datetime.today().strftime("%Y-%m-%d"+"_"+"%H:%M:%S"))

all_output_json={}
UPLOAD_FOLDER = './input_imagejsons'
URL='http://35.186.166.22:8082/portal/servlet/service/Poheader.poi'
TEMURL='http://34.80.26.185:8086/PO_Processing/GetDataSolr.Temp'
MATHURL='http://34.80.26.185:8086/PO_Processing_API3/ReArrangingofData'
OVERLAPURL='http://35.186.166.22:8082/portal/servlet/service/Poheader.poi'
RULEAMOUNTURL='http://35.186.166.22:8082/portal/servlet/service/Poheader.poi'
HEADERRULE='http://35.186.166.22:8082/portal/servlet/service/Poheader.poi'
FOOTERRULE='http://35.186.166.22:8082/portal/servlet/service/Poheader.poi'
FOOTERS_JSON={'http://bizlem.io/PurchaseOrderProcessing#CGST':'cgst',
                'http://bizlem.io/PurchaseOrderProcessing#SGST':'sgst',
                'http://bizlem.io/PurchaseOrderProcessing#IGST':'igst',
                'http://bizlem.io/PurchaseOrderProcessing#GrossTotal':'grandtotal'
                }
FOOTERS_LABELS=['total','round']

urlhash=r'(\#[A-Za-z].*)'


app=Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

if not os.path.exists(UPLOAD_FOLDER):
    os.mkdir(UPLOAD_FOLDER)

def allowed_file(filename, extensions):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in extensions
    
@app.route('/image_Datajson',methods=['POST'])
def update_json():
    files=request.files
    sdata=request.data
    final_json=[]
    image_json={}
    image_json2={}
    print ('files--',files)
    # file1 project6 output used for noheader API output and file2 is project4 output used for footer data
    if 'file1' not in files:
        return jsonify({'error':'No project6 file provided'}), 400    
    elif 'file1' in files:
        file = request.files['file1']
        if file and allowed_file(file.filename, ['json']):
            filename = secure_filename(file.filename)
            path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            file.save(path)
            with open(path) as f:
                image_json=json.load(f)
                
    if 'file2' not in files:
        # return jsonify({'error':'No project4 file provided'}), 400    
        print ('file2')
    elif 'file2' in files:
        file2 = request.files['file2']
        if file and allowed_file(file.filename, ['json']):
            filename = secure_filename(file2.filename)
            path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            file2.save(path)
            with open(path) as f:
                image_json2=json.load(f)            
    # get footer data from json 
    footer_index=[]
    footers=[]
    footerdata_lst=[]
    all_data2={}
    if image_json2:                
        # 1 .foll code is used to get only indexes of footer from neogeon project4
        if 'footer_info' in image_json2.keys():        
            if 'footer_table_only_url' in image_json2['footer_info'].keys():
                lst=image_json2['footer_info']['footer_table_only_url']
                # footer_index=sorted(list(set[i['line_no'] for i in lst if 'line_no' in i.keys()]))
                for i in lst:
                    if 'line_no' in i.keys():
                        if i['line_no'] not in footer_index:
                            footer_index.append(i['line_no'])
                # print ('find---',footer_index)
            # if footer_index:
                # footer_index.append(max(footer_index)+1)                    
        # 2. code is to get data from above indexes
        amt_in_words=[]
        amt_in_words_url="http://bizlem.io/PurchaseOrderProcessing#AmountWords"
        amt=""
        currency=""
        vendor_name=""
        city=""
        country=""
        if 'concatenation' in image_json2.keys():
            for itm in image_json2['concatenation']:
                if 'words' in itm.keys():
                    for ind,xy in enumerate(itm['words']):
                        if 'url' in xy.keys():
                            if 'line_index' in itm.keys():
                                if itm['line_index']<4:
                                    if xy['url']=='http://bizlem.io/PurchaseOrderProcessing#Ltd':
                                        vendor_name=[itm['words'][i]['word'] for i in range(0,ind+1,1)]
                                        vendor_name=" ".join(vendor_name)                    
                                    elif xy['url']=='http://bizlem.io/PurchaseOrderProcessing#Limited':
                                        vendor_name=[itm['words'][i]['word'] for i in range(0,ind+1,1)]
                                        vendor_name=" ".join(vendor_name)                    
                                    elif xy['url']=='http://bizlem.io/PurchaseOrderProcessing#Pvt_Ltd':
                                        vendor_name=[itm['words'][i]['word'] for i in range(0,ind+1,1)]
                                        vendor_name=" ".join(vendor_name)                    
                                    elif xy['url']=='http://bizlem.io/PurchaseOrderProcessing#Corporation':
                                        vendor_name=[itm['words'][i]['word'] for i in range(0,ind+1,1)]
                                        vendor_name=" ".join(vendor_name)                    
                                    elif xy['url']=='http://bizlem.io/PurchaseOrderProcessing#Engineers':
                                        vendor_name=[itm['words'][i]['word'] for i in range(0,ind+1,1)]
                                        vendor_name=" ".join(vendor_name)                    
                                    # if vendor_name:
                                    # print ('vendor_name---',vendor_name,xy['url'])
                                
                            if xy['url']=='http://bizlem.io/PurchaseOrderProcessing#AmountWords':
                                # amt_in_words.append(xy['word'])
                                amt_in_words=[itm['words'][i]['word'] for i in range(ind+1,len(itm['words']),1)]
                                if amt_in_words:
                                    if 'http://bizlem.io/PurchaseOrderProcessing#INR' in str(" ".join(amt_in_words)):
                                        currency='INR'
                                        print ('currency---',currency)
                                amt=str(" ".join(amt_in_words)).replace(":","").replace('INR',"").strip()                        
                                amt_in_words_url=xy['url']
                                break
                if itm['line_index'] in footer_index:
                    if 'words' in itm.keys():
                        for xy in itm['words']:                                        
                            xy.update({'y':itm['y'],'line_index':itm['line_index']})
                            footers.append(xy)
        # foll code is used to directly get data from Neugeon project4 output
        if 'footer_info' in image_json2.keys():        
            if 'footer_table_with_offset' in image_json2['footer_info'].keys():
                footerdata_lst=image_json2['footer_info']['footer_table_with_offset']
                
    footers_Desc_xsorted=sorted(footers,key=lambda i: (i['y'],i['x1']))
    # send footer data for processing
    print ('amt_in_words---',amt_in_words)
    # print ('amt2---',amt)
    # ----------------result on xsorted json------------------------------------------------
    final_json={}
    final_json2={}
    sorted_table_data=[]
    table_data_lst=[]
    # sort table_data on y and x1 before adding column no.
    if 'table_data' in image_json.keys():
        table_data_lst=copy.deepcopy(image_json['table_data'])
        all_output_json.update({'1_table_data':copy.deepcopy(image_json['table_data'])})
        
    
    # add column no 
    counter=1    
    for index,item in enumerate(table_data_lst):
        # item.update({"column":str(counter)})
        # counter+=1
        if 'type' in item.keys():
            if item['type']=="Header no data":
                item['data_x1']=item['header_x1']
                item['data_x2']=item['header_x2']
            if item['type']=="Data no header":
                item['header_x1']=item['data_x1']
                item['header_x2']=item['data_x2']
    
    sorted_table_data=sorted(table_data_lst,key=lambda i: (i['line_index'],i['data_x1']))
    
    for index,item in enumerate(sorted_table_data):
        item.update({"column":str(counter)})
        counter+=1
        
    all_output_json.update({'2_sorted_table_data':sorted_table_data})    
    
    if len(sorted_table_data)>1:
        final_json=overlap_combine(0,1,sorted_table_data)
        
    all_output_json.update({'3_overlap_combine':final_json})    
    
    # # remove header no data fromjson
    final_json=delete_items(final_json)
    
    # # method 1 or 2 is used to process header & footer 
    # # 1. method is written to process header and footer & send to VK's API
    # # # all_data=process_footer(footerdata_lst,final_json)
    # # # all_data['result'].update({amt_in_words_url:amt,'currency':currency,'vendor_name':vendor_name})
    # # # all_output_json.update({'4_final_headerfooter':all_data})    
    #OR 2.method is written to process header and footer using our final_json output & send to VK's API
    # processed_footer_lst=[]
    # # processed_footer_lst=footer_rule_engine(footers_Desc_xsorted,footer_index,final_json)
    # # processed_footer_lst=footer_righttoleft(footers_Desc_xsorted,footer_index,final_json)
    footerdata_lst2=[]
    # footerdata_lst2=get_footer_datalist(image_json2)
    all_data2=textamount_ruleengine(footerdata_lst,final_json,image_json2)
    all_data2['result'].update({amt_in_words_url:amt,'currency':currency,'vendor_name':vendor_name,'city':city,'country':country})
    all_output_json.update({'4_final_headerfooter':all_data2})    
    # # print ('f--data',all_data)    
    # all_data['result'].update({'footer_table_all_with_offset':processed_footer_lst})    
    # all_data2['result'].update({'amount_in_words':amt_in_words})
    # all_data2['result'].update({'footer_table_all_with_offset':all_data2['result']['footer_table_all_with_offset']})    
    # print ('all oput---',all_output_json)
    return jsonify(all_data2)
        
def textamount_ruleengine(footerdata_lst,final_json,image_json):
    final_header_lst=[]
    final_footer_lst=[]
    footer_json={}
    footer_lst=[]
    header_json={}
    header_lst=[]
    footer_lines=[]
    header_lines=[]
    result_json={}
    footer_start=0
    for index,item in enumerate(footerdata_lst):
        if 'line_no' in item.keys():
            if item['line_no'] not in footer_lines:
                footer_lines.append(item['line_no'])
                
    for index,item in enumerate(final_json):
        if 'line_index' in item.keys():
            if item['line_index'] not in header_lines:
                header_lines.append(item['line_index'])
    c=1            
    fkey=""
    footer_json={}
    footer_json_together={}
    # ====================================================
    # get footers
    # footers_data=[]
    # footer_indexes=[]
    # if header_lines:
        # footer_start=max(header_lines)+1
        # # get footer data >= footer start
    # if 'concatenation' in image_json.keys():
        # for index,itm in enumerate(image_json['concatenation']):
            # if 'line_index' in itm.keys():
                # if itm['line_index']>=footer_start:                
                    # if 'words' in itm.keys():            
                        # for ind,xy in enumerate(itm['words']):
                            # # if 'url' in xy.keys():                                
                                # # if isinstance(xy['url'],unicode):
                                    # # urlpatt=re.findall(urlhash,xy['url'].encode('utf8'))
                                    # # if urlpatt:
                                        # # print ('footer--url--',urlpatt)
                            # xy.update({'line_index':itm['line_index']})
                            # footers_data.append(xy)
                    # footer_indexes.append(itm['line_index'])
                    
    # print ('all footer indexes---',footer_indexes)
    # # sort footer data line by line                                
    # all_footer_rows=[]
    # for i in footer_indexes:
        # footer_row=[]
        # for item in footers_data:
            # if item['line_index']==i:
                # footer_row.append(item)
        # if footer_row:
            # sorted_row=sorted(footer_row,key=lambda i:i['x1'])
            # all_footer_rows.append(sorted_row)
            # # print ('row---',sorted_row)
    # # get right most column from rows
    # key=""
    # val=""
    # lno=""
    # footer_data_lst=[]
    # footer_json={}
    # for index,item in enumerate(all_footer_rows):
        # data=[i['word'] for i in item]
        # key=""
        # val=""
        # lno=""
        # # go to last column 
        # for ind,i in enumerate(item):
            # lno=i['line_index']
            # lst=[]
            # footer_json={}
            # if ind==len(item)-1:
                # # print ('word---',i['word'])
                # val=re.findall('(\d{1,})',str(i['word']))
                # if val:
                    # val=i['word']
                # else:
                    # val=""
            # else:
                # if isinstance(i['url'],unicode):
                    # urlpatt=re.findall(urlhash,i['url'].encode('utf8'))
                    # if urlpatt:
                        # if FOOTERS_JSON.get(i['url'],False)!=False:
                            # key=i['url']                    
                    # else:
                        # if str(i['word']) in FOOTERS_LABELS:
                            # key=str(i['word'])
                # else:
                    # if str(i['word'].encode('utf-8')) in FOOTERS_LABELS:
                        # key="###_"+str(i['word'])
            # # print ('key--val---',key,val)            
            # if key!="" and val!="":
                # # footer_json.update({str(lno)+"_"+key:val,})
                # footer_json.update({key:val,'line_index':str(lno)})

            # if footer_json:
                # lst.append(footer_json)
        # if lst:
            # footer_data_lst.append(lst)
    # print ('footer_data_lst----',footer_data_lst)
    # =========================================================================
    fc=1
    for i in footer_lines:
        footer_json={}
        # fc=1
        lno=0
        for index,item in enumerate(footerdata_lst):
            if 'line_no' in item.keys():    
                if item['line_no']==i:
                    lno=item['line_no']
                    print ('fitem---',item)                
                    for k,v in item.items():            
                        urlpatt=re.findall(urlhash,k.encode('utf8'))
                        if urlpatt:
                            fkey=str(k)
                            # print ('fkey---',fkey,item[fkey]['value'])
                            footer_json.update({fkey:str(item[fkey]['value'])})
                            footer_json_together.update({fkey:str(item[fkey]['value'])})
                            print ('fjson1---',footer_json)
                        elif k!='line_no':                    
                            if isinstance(v,dict):
                                fkey='unknown'+str(fc)
                                fc+=1                                        
                                footer_json.update({fkey:str(v['value'])})
                                footer_json_together.update({fkey:str(v['value'])})
                                print ('fjson2---',footer_json)
                            else:
                                fkey='unknown'+str(fc)
                                fc+=1                                        
                                footer_json.update({fkey:str(v)})
                                footer_json_together.update({fkey:str(v)})
                                print ('fjson3---',footer_json)
        if footer_json:
            footer_json.update({'line_no':str(lno)})        
            final_footer_lst.append(footer_json)
            # print ('fjson---',footer_json)
            
    # print ('fj----',footer_json_together)    
    # send all footers together to rule engine
    fvalid=""
    # strj={"Rule_Engine":"RuleTax","project_name":"POTax","user_name":"carrotrule_xyz.com","RawJson":footer_json_together}
    # strj={"Rule_Engine":"FooterPORule","project_name":"FooterPONew","user_name":"carrotrule_xyz.com","RawJson":footer_json_together}
    strj={"user_name": "carrotrule_xyz.com","project_name":"POFooter27Apr", "Rule_Engine":"FooterRule27Apr","RawJson":footer_json_together}
    
    print ('footerinput--',strj)
    logging.info('footerinput--')
    logging.info(strj)
    returned_json=requests.post(FOOTERRULE,data=json.dumps(strj)).json()
    print ('footeroutput--',returned_json)
    logging.info('footeroutput--')
    logging.info(returned_json)
    if returned_json:
        if 'Valid' in returned_json.keys():
            fvalid=returned_json['Valid']
    # add flag in footerjson and replace unknown with urls 
    for i in final_footer_lst:
        i.update({'valid':fvalid})
        for k,v in i.items():
            for j,m in returned_json.items():
                if k==m:
                    i.update({j:str(v)})
            
    # get headers
    hkey=""
    cou=1
    returned_json={}
    urls_list=[]
    urls_json={}
    print ('header lines---',header_lines)
    for line in header_lines:
        header_json={}
        cou=1    
        dcounter=1
        urls_list=[]
        for index,item in enumerate(final_json):
            # print ('item---',item)
            urls_json={}
            if 'line_index' in item.keys():
                if item['line_index']==line:
                    if isinstance(item['header'],unicode):
                        # for k,v in item.items():                        
                        # prefix='c'+str(cou)
                        prefix='c'+str(cou)
                        # cou+=1
                        # print ('li---',item['line_index'])
                        urlpatt=re.findall(urlhash,item['header'].encode('utf8'))
                        if urlpatt:
                            hkey=str(item['header']).encode('utf8')
                            # header_json.update({prefix+'_'+hkey:str(item['data'])})
                            # header_json.update({prefix+'_'+hkey:str(item['data'])})
                            if header_json.get(item['header'],False)==False:
                                # print ('p-----',item['data'].encode('ascii','ignore'))
                                # if item['data'].isalpha() :
                                # ['data'].encode('utf-8', errors='ignore').decode('utf8'))
                                # item['data'].encode('utf-8', errors='ignore')
                                header_json.update({hkey:str(item['data'].encode('ascii','ignore'))})
                            elif header_json.get(item['header'],False)!=False:
                                header_json[str(1)+'_'+item['header']]=header_json.pop(item['header'],str(header_json[item['header']]))
                                header_json.update({str(2)+'_'+item['header']:item['data'].encode('ascii','ignore')})
                                # item['data'].decode('utf8')
                                # print ('p2-----',item['data'].encode('ascii','ignore'))
                                            
                            # header_json.update({hkey:item['data'].encode('utf-8', errors='ignore')})    
                            # header_json.update({hkey:item['data'].encode('ascii','ignore')})    
                            urls_json.update({hkey:item['data'].encode('ascii','ignore')})    
                            
                            if str(urlpatt[0]).replace('#',"").lower()=='quantity':
                                splitdata=str(item['data']).split()
                                # print ('data split----',str(item['data']).split())
                                if len(splitdata)>1:
                                    for i,d in enumerate(splitdata):
                                        if i==0:
                                            skey=item['header']
                                        else:
                                            skey='quantity_split_'+str(i)
                                        sdata=str(d)
                                        header_json.update({skey:sdata})
                                        
                            if str(urlpatt[0]).replace('#',"").lower()=='rate':
                                splitdata=str(item['data']).split()
                                # print ('data split2----',str(item['data']).split())
                                if len(splitdata)>1:
                                    for i,d in enumerate(splitdata):
                                        if i==0:
                                            skey=item['header']
                                        else:
                                            skey='rate_split_'+str(i)
                                        sdata=str(d)
                                        header_json.update({skey:sdata})
                                        
                            if str(urlpatt[0]).replace('#',"").lower()=='hsn/sac':
                                splitdata=str(item['data']).split()
                                # print ('data split3----',str(item['data']).split())
                                if len(splitdata)>1:
                                    for i,d in enumerate(splitdata):
                                        if i==0:
                                            skey=item['header']
                                        else:
                                            skey='hsn_split_'+str(i)
                                        sdata=str(d)
                                        header_json.update({skey:sdata})
                                        
                            if str(urlpatt[0]).replace('#',"").lower()=='amountnumbers':
                                splitdata=str(item['data']).split()
                                # print ('data split4----',str(item['data']).split())
                                if len(splitdata)>1:
                                    for i,d in enumerate(splitdata):
                                        if i==0:
                                            skey=item['header']
                                        else:
                                            skey='amount_split_'+str(i)
                                        sdata=str(d)
                                        header_json.update({skey:sdata})
                                        # ,'x1':str(item['data_x1']),'x2':str(item['data_x2'])
                        else:
                            hkey='unknown'+str(cou)
                            cou+=1                        
                            header_json.update({hkey:str(item['data'].encode('ascii','ignore'))})
                            # ,'x1':str(item['data_x1']),'x2':str(item['data_x2'])
                            # .encode('utf-8', errors='ignore'))
                                    
                        if urls_json:
                            urls_list.append(urls_json)
                        
        array=""
        valid=""
        if header_json:
            header_json.update({'line_no':str(line)})
            # header_lst.append(header_json)
            # # strj={"Rule_Engine":"RuleTax","project_name":"POTax","user_name":"carrotrule_xyz.com","RawJson":header_json}
            # strj={"Rule_Engine":"RuleTax19Apr","project_name":"POTax19Apr","user_name":"carrotrule_xyz.com","RawJson":header_json}
            # strj={"user_name": "carrotrule_xyz.com","project_name":"POTax24Apr1", "Rule_Engine":"RuleTax24Apr1","RawJson":header_json}
            strj={"user_name": "carrotrule_xyz.com","project_name":"POTax26Apr", "Rule_Engine":"RuleTax26Apr","RawJson":header_json}
            
            # print ('headinput--',strj)
            logging.info('input---')
            logging.info(strj)
            returned_json=requests.post(HEADERRULE,data=json.dumps(strj)).json()
            # print ('headoutput--',returned_json)
            logging.info('output---')
            logging.info(returned_json)
            if returned_json:
                if 'Valid' in returned_json.keys():
                    valid=returned_json['Valid']
                    # print ('####hjson---',header_json)
                if 'Array' in returned_json.keys():
                    array=returned_json['Array']
                # chk for urls returned for unknown keys
                for k,v in header_json.items():
                    for j,m in returned_json.items():
                        if k==m:
                            header_json.update({j:str(v)})
            header_json.update({'valid':valid,'array':array})
            # print ('hjj----',header_json)
            final_header_lst.append(header_json)
    # chk flags and do the resp operations
    # print ('fhl----',final_header_lst)
    removelines_list=[]
    for ind,item in enumerate(final_header_lst):
        if 'array' in item.keys():
            if item['array']=="2":
                #2=Check Previous Line
                if ind>0:
                    # final_header_lst[ind]['line_no']==final_header_lst[ind-1]['line_no']
                    item.update({'line_no':final_header_lst[ind-1]['line_no']})                    
                    # send both line to rule engine
                    tmp={}
                    for k,v in item.items():
                        if k=='line_no':
                            tmp.update({'line_no':item['line_no']})                            
                        elif k!='array' and k!='valid':
                            tmp.update({k:v})                            
                    for k,v in final_header_lst[ind-1].items():
                        if k!='array' and k!='valid' and k!='line_no':
                            tmp.update({k:v})                                    
                    
                    # strj={"Rule_Engine":"RuleTax19Apr","project_name":"POTax19Apr","user_name":"carrotrule_xyz.com","RawJson":tmp}
                    strj={"user_name": "carrotrule_xyz.com","project_name":"POTax26Apr", "Rule_Engine":"RuleTax26Apr","RawJson":tmp}
                    # print ('headinput222--',strj)
                    returned_json=requests.post(HEADERRULE,data=json.dumps(strj)).json()
                    # print ('headoutput222--',returned_json)
                    if returned_json:
                        if 'Valid' in returned_json.keys():
                            valid=returned_json['Valid']
                        if 'Array' in returned_json.keys():
                            array=returned_json['Array']
                    tmp.update({'valid':valid,'array':array})
                    del final_header_lst[ind]
                    del final_header_lst[ind-1]
                    final_header_lst.append(tmp)
                # header_json.update({'valid':valid,'array':array})
                # final_header_lst.append(header_json)
            if item['array']=="-1":
                # -1=Remove/ignore same Line
                # if ind>0:    
                # removeline=final_header_lst[ind-1]['line_no']
                removeline=item['line_no']
                if removeline not in removelines_list:
                    removelines_list.append(removeline)
                # print ('lllll---',removelines_list)
                # remove same line
                # for indx,itemx in enumerate(final_header_lst):
                    # if 'line_no' in itemx.keys():
                        # if itemx['line_no']==removeline:
                            # del final_header_lst[indx]
    
    # remove lines with -1
    for ln in removelines_list:
        for indx,itemx in enumerate(final_header_lst):
            if 'line_no' in itemx.keys():
                if itemx['line_no']==ln:
                    del final_header_lst[indx]
    
    # remove split keys ,items    
    for indx,itemx in enumerate(final_header_lst):
        for k,v in itemx.items():
            if 'split' in k:
                del itemx[k]
            elif 'unknown' in k:
                del itemx[k]
            elif '1_' in k:
                del itemx[k]
            elif '2_' in k:
                del itemx[k]
            elif '3_' in k:
                del itemx[k]
    # ===================================================================================                
    result_json.update({'result':{'footer_table_all_with_offset':final_footer_lst,'header_table_all':sorted(final_header_lst,key=lambda i:i['line_no'])}})            
    # print ('fjson-----',final_json)            
    return result_json
    
def delete_items(final_json):
    for ind,item in enumerate(final_json):
        if 'type' in item.keys():
            if item['type']=="Header no data":
                final_json=delete_index(ind,final_json)
                # print ('in---',ind)
                delete_items(final_json)
    return final_json
    
def delete_index(ind,final_json):
    del final_json[ind]
    # print ('deleted--',ind)
    return final_json            
    
def overlap_combine(current,next,image_json):
    # rename keys to c1 for current and c2 for next
    
    combined_json1=image_json[current].copy()
    combined_json2=image_json[next].copy()
    combined_json3={}
    # print ('combined_json1---',combined_json1)
    # print ('combined_json2---',combined_json2)
    
    combined_json1['c1_no']=combined_json1.pop('column',combined_json1['column'])
    combined_json1['c1_type']=combined_json1.pop('type',combined_json1['type'])
    combined_json1['c1_datatype']=combined_json1.pop('data_type',combined_json1['data_type'])
    combined_json1['c1_lno']=combined_json1.pop('line_index',combined_json1['line_index'])
    combined_json1['c1_header']=combined_json1.pop('header',combined_json1['header'])
    
    combined_json2['c2_no']=combined_json2.pop('column',combined_json2['column'])
    combined_json2['c2_type']=combined_json2.pop('type',combined_json2['type'])
    combined_json2['c2_datatype']=combined_json2.pop('data_type',combined_json2['data_type'])
    combined_json2['c2_lno']=combined_json2.pop('line_index',combined_json2['line_index'])
    combined_json2['c2_header']=combined_json2.pop('header',combined_json2['header'])
    
    combined_json3.update({'c1_header':str(combined_json1['c1_header']),'c2_header':str(combined_json2['c2_header']),'c1_lno':str(combined_json1['c1_lno']),'c2_lno':str(combined_json2['c2_lno']),'c1_no':str(combined_json1['c1_no']),'c2_no':str(combined_json2['c2_no']),'c1_datatype':str(combined_json1['c1_datatype']),'c2_datatype':str(combined_json2['c2_datatype']),'c1_type':str(combined_json1['c1_type']),'c2_type':str(combined_json2['c2_type'])})
    # print ('input---',combined_json3)
    
    # strj={"Rule_Engine":"RuleNoHeader","project_name":"ProjectNoHeader","user_name":"carrotrule_xyz.com","RawJson":combined_json3}
    strj={"Rule_Engine":"NoHeaderNoDataRule","project_name":"PONoHeaderNoData","user_name":"carrotrule_xyz.com","RawJson":combined_json3}
    
    returned_json=requests.post(OVERLAPURL,data=json.dumps(strj)).json()
    # print ('overlap_combine--',returned_json)
    # # final_json={}
    # # # print ('curr--',image_json[current])
    # # # print ('next--',image_json[next])
    # # print ('===============')
    if 'Output' in returned_json.keys():
        if returned_json['Output']=='MERGE':
            # print ('mjson--',returned_json['Output'])
            merged_json=overlap_mergejson(current,next,image_json)
            if current<=len(merged_json)-2:
                # print ('current3--',current)
                final_json=overlap_combine(current,current+1,merged_json)
            else:
                return merged_json
                
        else:
            if next<=len(image_json)-2:
                final_json=overlap_combine(next,next+1,image_json)
            else:
                return image_json
    else:
        if next<=len(image_json)-2:
            final_json=overlap_combine(next,next+1,image_json)
        else:
            return image_json
                    
    return image_json

def overlap_mergejson(current,next,image_json):
    # min_x1=min(image_json[current]image_json[next])
    # merge two json entities
    header=""
    if image_json[next]['header']=="" and image_json[current]['header']!="":
        header=image_json[current]['header']
    elif image_json[current]['header']=="" and image_json[next]['header']!="":
        header=image_json[next]['header']
    elif image_json[current]['header']!="" and image_json[next]['header']!="":
        urlpatt1=re.findall(urlhash,image_json[current]['header'])
        urlpatt2=re.findall(urlhash,image_json[next]['header'])
        if urlpatt1:
            header=image_json[current]['header']
        elif urlpatt2:
            header=image_json[next]['header']
        else:
            header=str(image_json[next]['header'])+" "+str(image_json[current]['header'])
            
    # if image_json[current]['header']!="":
        # urlpatt2=re.findall(urlhash,image_json[current]['header'])
        # if urlpatt2:
            # header=image_json[current]['header']
        # else:
            # header=image_json[next]['header']
    # else:
        # header=image_json[next]['header']
            
    # print ('header--',header)
    image_json[next].update({'header':header,'type':'Overlap','data_x1':image_json[current]['data_x1'],'header_x1':image_json[current]['header_x1'],'data':image_json[current]['data']+"  "+image_json[next]['data']})
    # print (image_json[current],image_json[next])
    del image_json[current]
    return image_json

def process_footer(footerdata_lst,final_json):
    footer_json={}
    footer_lst=[]
    header_json={}
    header_lst=[]
    result_json={}
    footer_lines=[]
    header_lines=[]
    for index,item in enumerate(footerdata_lst):
        if 'line_no' in item.keys():
            if item['line_no'] not in footer_lines:
                footer_lines.append(item['line_no'])
            
    for index,item in enumerate(final_json):
        if 'line_index' in item.keys():
            if item['line_index'] not in header_lines:
                header_lines.append(item['line_index'])
    
    # for i in footer_lines:
        # for index,item in enumerate(footerdata_lst):
            # for k,v in item.items():
                # # print ('k---',k)
                # footer_json={}
                # urlpatt=re.findall(urlhash,k.encode('utf8'))
                # if urlpatt:
                    # footer_json.update({k:{"value":v['value'],"X1_Offset":"","X2_Offset":"","X1-X2":"","X2-X1":"","Y_Offset":"","Line_Offset":""}})
                    # if 'line_no' in item.keys():
                        # footer_json.update({'line_no':item['line_no']})
                # # if k=='line_no':
                    # # footer_json.update({k:v})
                # # print ('ff---',footer_json)
                # if footer_json:
                    # footer_lst.append(footer_json)
    
    # get footers
    for i in footer_lines:
        footer_json={}
        for index,item in enumerate(footerdata_lst):
            if 'line_no' in item.keys():
                if item['line_no']==i:
                    for k,v in item.items():
                        # print ('k---',k)
                        # footer_json={}
                        urlpatt=re.findall(urlhash,k.encode('utf8'))
                        if urlpatt:
                            footer_json.update({k:{"value":v['value'],"X1_Offset":"","X2_Offset":"","X1-X2":"","X2-X1":"","Y_Offset":"","Line_Offset":""}})
                            footer_json.update({'line_no':item['line_no']})
                        
        if footer_json:
            footer_lst.append(footer_json)
    # print ('flist---',footerdata_lst)        
    
    # for index,item in enumerate(final_json):
        # if 'header' in item.keys():
            # header_json={}
            # urlpatt=re.findall(urlhash,item['header'].encode('utf8'))
            # if urlpatt:
                # header_json.update({item['header']:{"value":item['data'],"X1_Offset":"","X2_Offset":"","X1-X2":"","X2-X1":"","Y_Offset":"","Line_Offset":""}})
                # if 'line_index' in item.keys():
                    # # print ('h----',item['header'])
                    # header_json.update({'line_no':item['line_index']})
            # if header_json:    
                # header_lst.append(header_json)
    for i in header_lines:
        header_json={}
        for index,item in enumerate(final_json):
            if 'line_index' in item.keys():
                if item['line_index']==i:
                    for k,v in item.items():                        
                        urlpatt=re.findall(urlhash,item['header'].encode('utf8'))
                        if urlpatt:
                            header_json.update({item['header']:{"value":item['data'],"X1_Offset":"","X2_Offset":"","X1-X2":"","X2-X1":"","Y_Offset":"","Line_Offset":""}})
                            header_json.update({'line_no':item['line_index']})
                        
        if header_json:
            header_lst.append(header_json)
                
    result_json.update({'result':{'footer_table_all_with_offset':footer_lst,'header_table_all':header_lst}})    
    return result_json

def footer_rule_engine(footers_Desc_xsorted,footer_index,final_json):
    processed_footer_lst=[]
    all_rows=[]
    row=[]
    final_json_lines=[]
    
    for i in final_json:
        if i['line_index'] not in final_json_lines:
            final_json_lines.append(i['line_index'])
            
    for i in footer_index:
        row=[]
        for index,item in enumerate(footers_Desc_xsorted):
            if 'line_index' in item.keys():
                if item['line_index']==i:
                    row.append(item)
        if row:
            all_rows.append(row)
            # print ('row--',row)
    # header process
    row2=[]
    all_rows2=[]
    for i in final_json_lines:
        row2=[]
        for index,item in enumerate(final_json):
            if item['line_index']==i:
                if 'type' in item.keys():
                    if item['type']=='Overlap':
                        row2.append(item)
                        
        if row2:            
            all_rows2.append(row2)
    
    # form data in structure to send rule engine
    url="N"
    counter=0
    for i in all_rows:
        temp={}
        counter=0
        for j in i:
            urlpatt=re.findall(urlhash,j['url'].encode('utf8'))
            if urlpatt:
                url='Y'
            else:
                url='N'
            temp.update({'w'+counter:j['word'],'w'+counter+'_x1':j['x1'],'w'+counter+'_x2':j['x2'],'w'+counter+'_datatype':j['type'],'w'+counter+'_url':url})
            counter+=1
            print ('temp---',temp)        
    
    return all_rows
    
def footer_righttoleft(footers_Desc_xsorted,footer_index,final_json):
    row=[]
    all_rows=[]
    for i in footer_index:
        row=[]
        for index,item in enumerate(footers_Desc_xsorted):
            if item['line_index']==i:
                row.append(item)
        if row:
            # print ('#####',row)
            all_rows.append(sorted(row,key=lambda i:i['x1'],reverse=True))
    
    value=""    
    key=""
    x2=0
    footer_dic={}
    lst=[]
    counter=1
    for index,item in enumerate(all_rows):
        data=[im['word'] for im in item] 
        value=""
        key="unknown"
        lno=0
        counter=1
        if footer_dic:
            lst.append(footer_dic)
        footer_dic={}
        lno=item[0]['line_index']
        # footer_dic.update({'line_no':lno})
        for ind,i in enumerate(item):
            # print ('i--',i)
            if ind==0:
                if i['word']!="":
                    val=re.findall('(\d{1,})',str(i['word']))
                    x2=i['x2']
                    if val:
                        # print ('val--',val,i['x1'],i['x2'])
                        value=i['word']
            else:
                if i['word']!="":
                    val=re.findall('(\d{1,})',str(i['word']))
                    if val:
                        if x2!=0:
                            if i['x2']in range(x2-3,x2+3,1):
                                # print ('val--',val,i['x1'],i['x2'])
                                value=i['word']
                                key='unknown'
                
            if index>0:
                # if value!="":
                if i['url']!="":
                    urlpatt=re.findall(urlhash,str(i['url']))
                    if urlpatt:
                        key=i['url']
                        
            # if key=="":
                # key=='unknown'
            # if key!="" and     value!="":
            # print ('key--val--',key,value)
            if key=='unknown':
                key+=str(counter)
                counter+=1
            footer_dic.update({key:{"value":value,"X1_Offset":"","X2_Offset":"","X1-X2":"","X2-X1":"","Y_Offset":"","Line_Offset":""}})    
                    
    
    # print ('lst---',lst)
    return lst
    
if __name__=='__main__':
    app.run(host='0.0.0.0',debug=True,port=5023)

import json
import requests
import threading
import re
import traceback
from tqdm import tqdm

URL = 'http://3.84.244.86:8080/enhancer/chain/PurchaseOrderV3'
FLATTEN_URL = 'http://34.74.243.55:8086/PO_Processing/GetDataSolr.flatteringTemp'
CHOOSE_URL = 'http://34.74.243.55:8086/PO_Processing/GetDataSolr.Temp'
REGEX_URL = 'http://34.74.142.84:5020/regexResult'

LABEL = 'http://fise.iks-project.eu/ontology/entity-reference'
TEXT_LABEL = 'http://fise.iks-project.eu/ontology/selected-text'
RELATION_LABEL = 'http://purl.org/dc/terms/relation'
SELECTED_LABEL = 'http://bizlem.io/PurchaseOrderProcessing#'
SITE_LABEL = 'http://stanbol.apache.org/ontology/entityhub/entityhub#site'

def middle(value, left, right):
    return value >= left and value <= righft

def get_x(obj, index=0):
    return obj['bounding_poly']['vertices'][index]['x']

def get_y(obj, index=0):
    return obj['bounding_poly']['vertices'][index]['y']

def to_float(s):
    s_format = ''.join(re.split('[^\d\.]', s))
    return float(s_format)

def is_float(s):
    s_format = ''.join(re.split(',', s))
    try:
        num = float(s_format)
        return True
    except ValueError:
        return False

def strip(s):
    return ''.join(re.split('[^a-zA-Z0-9]', s.lower()))


def formatJSON(data):
    new_data = []
    for item in data:
        new_data.append({
            'description': item['text'],
            'bounding_poly': {
                'vertices': [ item['position'] ] * 4,
            },
        })
    return new_data

def parseJSON(data, x_thres, y_thres,
    word_special_chars=set(['+', '-', ';', '/', '\\', "'", '"']),
    number_special_chars=set([',', '.']),
):
    number_special_chars.add(',')
    number_special_chars.add('.')

    response = []

    # Build histogram
    y = [ (get_y(obj) + get_y(obj, 2)) // 2 for obj in data ]
    y.sort()

    cur_y = y[0]
    hist_y = { cur_y: cur_y }

    for yc in y:
        if yc == cur_y:
            continue
        if yc - cur_y > y_thres:
            cur_y = yc
        hist_y[yc] = cur_y

    data.sort(key=lambda obj: hist_y[(get_y(obj) + get_y(obj, 2)) // 2] * 1000000 + get_x(obj))


    curLineYCoord = (get_y(data[0]) + get_y(data[0], 2)) // 2
    curXEnd = get_x(data[0], 2)
    curXStart = get_x(data[0])
    line = {
        'y': curLineYCoord,
        'words': [],
    }

    completedWord = data[0]['description']
    characters = set(word_special_chars)
    open_chars = set(['(', '[', '{'])
    close_chars = set([')', ']', '}'])

    for i in range(1, len(data)):
        curWord = data[i]['description']
        yMid = (get_y(data[i]) + get_y(data[i], 2)) // 2
        xStart = get_x(data[i])


        if hist_y[yMid] <= hist_y[curLineYCoord] + y_thres and hist_y[yMid] >= hist_y[curLineYCoord] - y_thres:
            if xStart <= curXEnd + x_thres \
                or (xStart <= curXEnd + 2 * x_thres and ( \
                    (curWord[0] in characters) \
                    or curWord[0] in close_chars \
                    or ((curWord[0] in set(list(number_special_chars) + ['%'])) # need char % \
                        and '0' <= completedWord[-1] and completedWord[-1] <= '9') \
                    or completedWord[-1] in characters \
                    or completedWord[-1] in open_chars \
                    or (len(completedWord) >= 2 and completedWord[-1] in set(number_special_chars) \
                        and '0' <= curWord[0] and curWord[0] <= '9' \
                        and '0' <= completedWord[-2] and completedWord[-2] <= '9'))):
                    completedWord += curWord
                    curXEnd = get_x(data[i], 2)
            else:
                line['words'].append({
                    'word': completedWord,
                    'x1': curXStart,
                    'x2': curXEnd,
                })

                curXEnd = get_x(data[i], 2)
                completedWord = data[i]['description']
                curXStart = get_x(data[i])
        else:
            line['words'].append({
                'word': completedWord,
                'x1': curXStart,
                'x2': curXEnd,
            })

            response.append(line)
            curLineYCoord = (get_y(data[i]) + get_y(data[i], 2)) // 2
            curXStart = get_x(data[i])
            curXEnd = get_x(data[i], 2)

            completedWord = data[i]['description']
            line = {
                'y': curLineYCoord,
                'words': [],
            }

    for line_index in range(len(response) - 1, 0, -1):
        current_line = response[line_index]
        prev_line = response[line_index - 1]

        if len(current_line['words']) == 0: continue
        if current_line['y'] - prev_line['y'] > y_thres * 2: continue

        x_left = current_line['words'][0]['x1']
        x_right = 0
        if len(prev_line['words']) > 0:
            x_right = prev_line['words'][-1]['x2']

        if x_right < x_left:
            prev_line['words'] += current_line['words']
            current_line['words'] = []

    response = [ line for line in response if len(line['words']) > 0 ] 

    return response

def p4_process_json(path,
        verbose=True,
        x_thres=0,
        y_thres=10,
        word_special_chars=[],
        number_special_chars=[],
        required_urls=[],
        regex_line_step=2,
        mode='normal',
    ):
    
    header_info = {}
    result = {}
    footer_info = {}

    with open(path, encoding='utf-8') as f:
        data = json.load(f)

        if mode == 'normal':
            data_sum = data[0]
            data = data[1:]
        elif mode == 'pdf':
            data = formatJSON(data)

    slist = parseJSON(data, x_thres, y_thres,
        word_special_chars=set(word_special_chars),
        number_special_chars=set(number_special_chars),
    )


    max_y = int(slist[-1]['y'])
    max_x = 0
    for line in slist:
        if len(line['words']) == 0:
            continue

        last_word = line['words'][-1]['x2']
        if last_word > max_x:
            max_x = last_word

    sres = []
    sres_words = []

    for line in slist:
        sres.append({
            'y': line['y'],
            'words': [ word['word'] for word in line['words'] ],
        })

        sres_words.append({
            'y': line['y'],
            'words': [ word['word'] for word in line['words'] ]
        })


    def request_line_and_replace(slist, sres, sres_words, line_index):
        sentence = ' '.join([ word for word in sres[line_index]['words'] ])
        res = {}
        try: 
            r = requests.post(
                URL, 
                data=sentence.encode('utf-8'), 
                headers={'Content-Type': 'application/pdf'}
            )
            r = r.json()

            for obj in r:
                if LABEL in obj and RELATION_LABEL in obj:
                    res[obj[LABEL][0]['@id']] = obj[RELATION_LABEL][0]['@id']

            line_type = []
            for obj in r:
                if TEXT_LABEL in obj and '@id' in obj:
                    obj_id = obj['@id']
                    for kurl, vid in res.items():
                        if vid == obj_id:
                            res[kurl] = obj[TEXT_LABEL][0]['@value']
                            
                if SITE_LABEL in obj:
                    line_type.append(obj[SITE_LABEL][0]['@value'])
           
            merging_r = {}
            words = slist[line_index]['words']
            
            word_index = 0
            while word_index < len(words):
                word = words[word_index]
                found = False
                
                for kurl, vtext in res.items():
                    if kurl in merging_r:
                        continue
                        
                    if found:
                        break

                    vtext_first_word = vtext.split(' ')[0]
                    if vtext_first_word not in word['word']:
                        continue
                        
                    current_word_indexes = [ word_index ]
                    current_s = word['word']
                    current_i = word_index + 1
                    while len(current_s) < len(vtext) and current_i < len(words):
                        current_word_indexes.append(current_i)
                        current_s += ' ' + words[current_i]['word']
                        current_i += 1
                    
                    if vtext not in current_s:
                        continue
                    
                    found = True
                    merging_r[kurl] = current_word_indexes
                    
                if not found:
                    word_index += 1


            removed_words = set()
            for url, merging_arr in merging_r.items():
                min_index = merging_arr[0]
                start_word = slist[line_index]['words'][min_index]
                sres_words[line_index]['words'][min_index] = url

                for word_index in merging_arr[1:]:
                    removed_words.add(word_index)
                    current_word = slist[line_index]['words'][word_index]
                    start_word['word'] += ' ' + current_word['word']
                    start_word['x2'] = current_word['x2']

            slist_newarr = []
            sres_words_newarr = []

            for word_index, word in enumerate(slist[line_index]['words']):
                if word_index in removed_words: continue

                slist_newarr.append(word)
                sres_words_newarr.append(sres_words[line_index]['words'][word_index])

            slist[line_index]['words'] = slist_newarr
            slist[line_index]['type'] = line_type
            sres_words[line_index]['words'] = sres_words_newarr

            sres[line_index]['words'] = list(res.keys())

        except Exception as e:
            traceback.print_exc()

    def request_all():
        threads = [ threading.Thread(
            target=request_line_and_replace, 
            args=(slist, sres, sres_words, line_index),
        ) for line_index in range(len(sres)) ]
        for thread in threads: thread.start()
        for thread in tqdm(threads): thread.join()

    if verbose: print('requesting to get urls...')
    request_all()
    if verbose: print('finish requesting...')


    # for line_index, line in enumerate(slist):
    #     sentence = [ word['word'] for word in line['words'] ]
    #     print(line_index, line['y'], sentence, line['type'])
    # return

    debug = []
    for line_index, line in enumerate(slist):
        words = []
        for word_index, word in enumerate(line['words']):
            url = sres_words[line_index]['words'][word_index]
            if SELECTED_LABEL not in url:
                words.append({
                    'word': word['word'],
                    'x1': word['x1'],
                    'x2': word['x2'],
                    'type': 'number' if is_float(word['word']) else 'string',
                    'url': None,
                })
            else:
                words.append({
                    'word': word['word'],
                    'x1': word['x1'],
                    'x2': word['x2'],
                    'type': 'number' if is_float(word['word']) else 'string',
                    'url': url,
                })
        debug.append({
            'y': line['y'],
            'line_index': line_index,
            'number_of_words': len(words),
            'words': words,
        })


    def concat(slist, line_index, index, n_lines=2):
        paragraph = ' '.join([ word['word'] for word in slist[line_index]['words'][index:] ]) + '\n'
        current_word = slist[line_index]['words'][index]
        
        for line in slist[line_index + 1:(line_index + 1) + n_lines]:
            for word in line['words']:
                if word['x1'] >= current_word['x1'] - 15:
                    paragraph += word['word'] + ' '
            paragraph += '\n'
        
        return paragraph


    date_regex = '\d{1,2}\s?[\.\-\/\:]\s?\d{1,2}\s?[\.\-\/\:]\s?\d{4}|\d{1,2}[\s\-]+[A-Za-z]{3,}[\s\-]+\d{4}|\d{1,2}\s?\/\s?\d{1,2}\s?\/\s?\d{2}'
    # (MM.DD.YYYY) | MM - DDD - YYYY | MM - DD - YYYY | MM / DD / YY
    number_regex = '(?<![\.\d])(?:\,? ?\d+ ?)+(?:\. ?\d+)?(?!\d{0,} ?%)'
    percentage_regex = '\d+\s?%|\d+\s?\.\s?%'

    # processing text
    patterns = {
        'PAN_Number': [{
            'regex': '[A-Z]{5}[0-9]{4}[A-Z]{1}',
            'main_key': 'PAN_Number',
        }],
        'TAN': [{
            'regex': '[A-Z]{4}[0-9]{5}[A-Z]{1}',
            'main_key': 'TAN',
        }],
        'Purchase_Order_Number': [{
            'regex': '\d{1,}',
            'main_key': 'Purchase_Order_Number',
        }],
        'GSTIN': [{
            'regex': '[A-Z0-9]{15}',
            'main_key': 'GSTIN',
        }],
        'Invoice_Number': [{
            'regex': '\d{2,}|[A-Z]+\d+[A-Z]\s?(\d{2})?\s?\-\s?\d+|[A-Z]\s?\-?\s?\d+|[A-Z]+\/\d+\-\d+\/\d+(\/\d+)?',
            'main_key': 'Invoice_Number',
        }],
        # 12345 | SA18Y 09 - 00205 | S - 168 | GST/18-19/025
        'Invoice_Date': [{
            'regex': date_regex,
            'main_key': 'Invoice_Date',
        }],
        'Date': [{
            'regex': date_regex,
            'main_key': 'Invoice_Date',
        }],
        'L.R._No': [{
            'regex': '\d{1,2}\s?[(th)\-\.]?\s?([A-Za-z]{3,})\s?[\-\.]?\s?\d{4}|\d{1,2}\s?\.\s?\d{1,2}\s?\.\s?\d{2}|BY MONTH\s?\/?\s?\d{4}\s?\-\s?\d{4}',
            'main_key': 'L.R._No',
        }],
        'L.R._Dt': {
            'regex': date_regex,
            'main_key': 'L.R._Dt',
        },
        'GrossTotal': [{
            'regex': number_regex,
            'main_key': 'GrossTotal',
        }],
    }


    patterns_reversed = {
        'SGST': [
            {
                'regex': number_regex,
                'main_key': 'SGST_Amount',
            },
            {
                'regex': percentage_regex,
                'main_key': 'SGSTPercentage',
            },
        ],
        'CGST': [
            {
                'regex': number_regex,
                'main_key': 'CGST_Amount',
            },
            {
                'regex': percentage_regex,
                'main_key': 'CSGTPercentage',
            },
        ],
        'IGST': [
            {
                'regex': number_regex,
                'main_key': 'IGST_Amount',
            },
            {
                'regex': percentage_regex,
                'main_key': 'IGSTPercentage',
            },
        ],
        'SGSTPercentage': [{
            'regex': percentage_regex,
            'main_key': 'SGSTPercentage'
        }],
        'CGSTPercentage': [{
            'regex': percentage_regex,
            'main_key': 'CGSTPercentage'
        }],
        'IGSTPercentage': [{
            'regex': percentage_regex,
            'main_key': 'IGSTPercentage'
        }],
    }

    result_keys = [
        'TaxInvoice',
        'Invoice_Number',
        'Ship_To',
        'Bill_To',
        'EssarGSTIN',
        'Hazira',
        'Invoice_Date',
        'PAN_Number',
        'TAN',
        'Purchase_Order_Number',
        'GSTIN',
        'L.R._No',
        'L.R._Dt',
        'Transport',
        'SERVICES_TAX_REG_NO',
        'SGST_Amount',
        'CGST_Amount',
        'IGST_Amount',
        'CGSTPercentage',
        'SGSTPercentage',
        'IGSTPercentage',
        'GrossTotal',
    ]

    number_keys = [
        'SGST_Amount',
        'CGST_Amount',
        'IGST_Amount',
    ]

    gross_total = 0
    for key in result_keys:
        result[SELECTED_LABEL + key] = None

    for line_index, line in enumerate(slist):
        if len(sres[line_index]['words']) == 0:
            continue

        for word_index, word in enumerate(slist[line_index]['words']):
            word_url = sres_words[line_index]['words'][word_index]
            if SELECTED_LABEL not in word_url:
                continue

            word_url_key = word_url.split('#')[-1]

            if word_url_key == 'TaxInvoice':
                result[word_url] = True
                continue

            if word_url_key not in patterns: continue

            paragraph = concat(slist, line_index, word_index)
            pattern_list = patterns[word_url_key]

            for pattern in pattern_list:
                result_labels = set([ label.split('#')[-1] for label in result.keys() if result[label] is not None ])

                # ignore already exist key in result
                if pattern['main_key'] in result_labels:
                    continue

                if verbose:
                    print(line_index, line['y'], pattern['main_key'])

                if pattern['main_key'] == 'GrossTotal':
                    for sub_line in slist[line_index:line_index + 3]:
                        start_index = 0
                        for index, word in enumerate(sub_line['words']):
                            if word['x1'] > max_x * 0.7:
                                start_index = index
                                break

                        sentence = ' '.join([ word['word'] for word in sub_line['words'][start_index:] ])
                        search_result = [ to_float(num) for num in re.findall(pattern['regex'], sentence) ]
                        if len(search_result) > 0 and max(search_result) > gross_total:
                            gross_total = max(search_result)

                else:
                    search_p = re.search(pattern['regex'], paragraph)
                    if search_p is not None:
                        result[SELECTED_LABEL + pattern['main_key']] = search_p.group(0)


    result[SELECTED_LABEL + 'GrossTotal'] = gross_total


    for line_index in range(len(slist) - 1, -1, -1):
        if len(sres[line_index]['words']) == 0:
            continue

        for word_index, word in enumerate(slist[line_index]['words']):
            word_url = sres_words[line_index]['words'][word_index]
            if SELECTED_LABEL not in word_url:
                continue

            word_url_key = word_url.split('#')[-1]
            if word_url_key not in patterns_reversed: continue

            paragraph = concat(slist, line_index, 0)
            pattern_list = patterns_reversed[word_url_key]

            for pattern in pattern_list:
                result_labels = set([ label.split('#')[-1] for label in result.keys() if result[label] is not None ])

                # ignore already exist key in result
                if pattern['main_key'] in result_labels:
                    continue

                if verbose: print(line_index, line['y'], pattern['main_key'])

                search_p = re.search(pattern['regex'], paragraph)
                if search_p is not None:
                    result[SELECTED_LABEL + pattern['main_key']] = search_p.group(0)

    for key in number_keys:
        if result[SELECTED_LABEL + key] is not None:
            result[SELECTED_LABEL + key] = to_float(result[SELECTED_LABEL + key])


    # detect tax table (only 1 line)
    # tax_table_line_index = line_index
    # tax_table_info = { 'tax_table_line_index': [] }
    # for line_index, line in enumerate(slist):
    #     if 'InvTableHeaders' in line['type'] or 'InvoiceFooter' in line['type']: continue
    #     if 'InvoiceTax' in line['type'] or 'InvoiceAmount' in line['type']:
    #         tax_table_info['tax_table_line_index'].append(line_index)
    #         tax_table_line_index = line_index
    #         break

    # detect headers
    def is_header(line_index):
        line_type = slist[line_index]['type']
        if 'InvoiceFooter' in line_type or len(line_type) == 0:
            return False
        return True

    candidate_header_indexes = []
    for line_index, line in enumerate(slist):
        # if line_index == tax_table_line_index: continue 
        if is_header(line_index):
            candidate_header_indexes.append(line_index)

    # detect header cluster
    center_header_index = None
    for lindex in candidate_header_indexes:
        start = max(0, lindex - 2)
        stop = min(len(slist), lindex + 3)

        count_headers = 0
        for line_index in range(start, stop):
            for line_type in slist[line_index]['type']:
                if line_type == 'InvTableHeaders':
                    count_headers += 1

        if count_headers >= 2:
            center_header_index = lindex 
            break

    header_line_index = []
    for line_index in range(max(0, center_header_index - 2), min(len(slist), center_header_index + 3)):
        if is_header(line_index):
            header_line_index.append(line_index)

    header_info['header_line_index'] = header_line_index

    # detect footers
    def is_footer(line_index):
        line_type = slist[line_index]['type']
        if len(line_type) != 1: return False
        if 'InvTableHeaders' in line_type or 'PurchaseEntity1' in line_type:
            return False
        return True

    footer_line_index = []
    start_index = header_line_index[-1] if len(header_line_index) > 0 else 0
    for lindex, line in enumerate(slist[start_index:]):
        line_index = start_index + lindex
        # if line_index == tax_table_line_index: continue
        if is_footer(line_index):
            footer_line_index.append(line_index)


    footer_info['footer_line_index'] = footer_line_index

    # Regex part
    def concat_rows_and_cols(slist, line_index, word_index, n_lines=10, no_limit=False):
        paragraph = []
        for word in slist[line_index]['words'][word_index:]:
            nword = word.copy()
            nword['y'] = slist[line_index]['y']
            nword['line_index'] = line_index
            paragraph.append(nword)
        
        current_word = slist[line_index]['words'][word_index]
        
        for lindex in range(line_index + 1, min(line_index + n_lines + 1, len(slist))):
            line = slist[lindex]
            for word in line['words']:
                if no_limit or (word['x1'] >= current_word['x1'] - 15 and word['x1'] <= current_word['x2']):
                    nword = word.copy()
                    nword['y'] = line['y']
                    nword['line_index'] = lindex
                    paragraph.append(nword)
        
        return paragraph

    def request_regex_word(word, word_index, container):
        try:
            r = requests.post(REGEX_URL, data=word['word'].encode('utf-8'))
            container[word_index] = r.json()
        except Exception as e:
            traceback.print_exc()

    def request_regex_header(slist, line_index, word_index, word_url, paragraph, regex_res):        
        container = [ None ] * len(paragraph)
        threads = [ threading.Thread(
            target=request_regex_word, 
            args=(word, word_index, container)
        ) for word_index, word in enumerate(paragraph) ]

        for thread in threads: thread.start()
        for thread in threads: thread.join()

        regex_res.setdefault(word_url, {
            'Words': [],
            'Regex': [],
            'Integer': [],
            'Date': [],
            'UKS': [],
        })
        
        current_word = slist[line_index]['words'][word_index]

        for i in range(max(0, line_index - regex_line_step), min(len(slist), line_index + regex_line_step + 1)):
            for word in slist[i]['words']:
                regex_res[word_url]['Words'].append({
                    'Value': word['word'],
                    'Length': len(word['word']),
                    'Line_Offset': i - line_index,
                    'X1 OffSet': word['x1'] - current_word['x1'],
                    'X1-X2 OffSet': word['x1'] - current_word['x2'],
                    'Y OffSet': slist[i]['y'] - slist[line_index]['y'],
                    'X2-X1 OffSet': word['x2'] - current_word['x1'],
                    'X2-X2 OffSet': word['x2'] - current_word['x2'],
                })
                
        for rr_index, rr in enumerate(container):
            word = paragraph[rr_index]
            for item in rr:
                regex_res[word_url]['Regex'].append({
                    'Regex_Type': item['Regex_Type'],
                    'Regex_Value': item['value'],
                    'Regex_Id': item['Regex_id'],
                    'Regex_Length': item['length'],
                    'Regex_Line_OffSet': word['line_index'] - line_index,
                    'Regex_X1 OffSet': word['x1'] - current_word['x1'],
                    'Regex_X1-X2 OffSet': word['x1'] - current_word['x2'],
                    'Regex_Y OffSet': word['y'] - slist[line_index]['y'],
                    'Regex_X2-X1 OffSet': word['x2'] - current_word['x1'],
                    'Regex_X2-X2 OffSet': word['x2'] - current_word['x2'],
                })
                
        for word in paragraph:
            search_result = re.search('\d+', word['word'])
            if search_result is not None:
                regex_res[word_url]['Integer'].append({
                    'Integer_Value': word['word'][search_result.start():search_result.end()],
                    'Integer_Length': search_result.end() - search_result.start(),
                    'Integer_Line_OffSet': word['line_index'] - line_index,
                    'Integer_X1 OffSet': word['x1'] - current_word['x1'],
                    'Integer_X1-X2 OffSet': word['x1'] - current_word['x2'],
                    'Integer_Y OffSet': word['y'] - slist[line_index]['y'],
                    'Integer_X2-X1 OffSet': word['x2'] - current_word['x1'],
                    'Integer_X2-X2 OffSet': word['x2'] - current_word['x2'],
                })

            search_result_date = re.search(date_regex, word['word'])
            if search_result_date is not None:
                regex_res[word_url]['Date'].append({
                    'Date_Value': word['word'][search_result_date.start():search_result_date.end()],
                    'Date_Length': search_result_date.end() - search_result_date.start(),
                    'Date_Line_OffSet': word['line_index'] - line_index,
                    'Date_X1 OffSet': word['x1'] - current_word['x1'],
                    'Date_X1-X2 OffSet': word['x1'] - current_word['x2'],
                    'Date_Y OffSet': word['y'] - slist[line_index]['y'],
                    'Date_X2-X1 OffSet': word['x2'] - current_word['x1'],
                    'Date_X2-X2 OffSet': word['x2'] - current_word['x2'],
                })


    regex_urls = required_urls

    threads = []
    regex_res = {}
    for line_index, line in enumerate(slist):
        for word_index, word in enumerate(line['words']):
            word_url = sres_words[line_index]['words'][word_index]
            if word_url in regex_urls:
                paragraph = concat_rows_and_cols(slist, line_index, word_index)
                
                threads.append(threading.Thread(
                    target=request_regex_header, 
                    args=(slist, line_index, word_index, word_url, paragraph, regex_res),
                ))
    
    if verbose: print('requesting for regex...')
    for thread in threads: thread.start()
    for thread in threads: thread.join()
    if verbose: print('finish requesting')

    header_info['Rules'] = regex_res
            

    return {
        'result': result,
        'header_info': header_info,
        'footer_info': footer_info,
        # 'tax_table_info': tax_table_info,
        'concatenation': debug,
    }


if __name__ == '__main__':
    r = p4_process_json('p4materials/c47.json', verbose=True)
    print(json.dumps(r, indent=2))


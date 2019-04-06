import json
import requests
import threading
import re

URL = 'http://35.188.227.39:8080/enhancer/chain/ReceiptChain'
LABEL = 'http://fise.iks-project.eu/ontology/entity-reference'
SELECTED_LABEL = 'http://bizlem.io/PurchaseOrderProcessing#'

def middle(value, left, right):
    return value >= left and value <= right

def get_x(obj, index=0):
    return obj['bounding_poly']['vertices'][index]['x']

def get_y(obj, index=0):
    return obj['bounding_poly']['vertices'][index]['y']

def get_box(obj):
    left_x = max(get_x(obj, 0), get_x(obj, 3))
    right_x = min(get_x(obj, 1), get_x(obj, 2))
    top_y = max(get_y(obj, 0), get_y(obj, 1))
    bot_y = min(get_y(obj, 2), get_y(obj, 3))
    
    return (left_x, top_y, right_x, bot_y)

def get_x_range(arr_obj, boxes):
    left_x, _, _, _ = boxes[arr_obj[0]]
    _, _, right_x, _ = boxes[arr_obj[-1]]
    return (left_x, right_x)

def to_float(s):
    s_format = ''.join(re.split('[^\d\.]', s))
    return float(s_format)

def process_json(path):
    with open(path) as f:
        data = json.load(f)
        data_sum = data[0]
        data = data[1:]

        max_x = 0
        max_y = 0
        
    # calculate boxes
    boxes = []
    for obj in data:
        boxes.append(get_box(obj))

    # find max_x and max_y
    for i, obj in enumerate(data):
        _, _, right_x, bot_y = boxes[i]
        max_x = max(max_x, right_x)
        max_y = max(max_y, bot_y)
        
    print(max_x, max_y, len(data))
    height = max_y + 1
    width = max_x + 1
    coor = [[ None for x in range(width) ] for y in range(height)] 

    # map to coor
    for index, obj in enumerate(data):
        left_x, top_y, right_x, bot_y = boxes[index]

        for y in range(top_y, bot_y + 1):
            for x in range(left_x, right_x + 1):
                if coor[y][x] is not None:
                    continue
                coor[y][x] = index

    slist = {}
    visited = []

    MARGIN_X = 5
    MARGIN_Y = 8

    print('processing matrix...')
    for x in range(width):
        for y in range(height - 1, -1, -1):
            if coor[y][x] is None or coor[y][x] in visited:
                continue
                
            index_obj = coor[y][x]
            left_x, top_y, _, bot_y = boxes[index_obj]
            obj = data[index_obj]
            visited.append(index_obj)
            
            # test range y within MARGIN_Y
            row = y
            MARGIN_Y = int((bot_y - top_y) * 0.6)
            for i in range(y - MARGIN_Y, y + MARGIN_Y + 1):
                if i in slist.keys():
                    row = i
                    break
            
            if row not in slist.keys():
                slist[row] = []
                
            if len(slist[row]) > 0:
                prev_words = slist[row][-1]
                prev_word = prev_words[-1]
                pw_left_x, pw_top_y, pw_right_x, pw_bot_y = boxes[prev_word]
                
                MARGIN_X = int((pw_bot_y - pw_top_y) * 0.7)
                
                if pw_right_x + MARGIN_X >= left_x:
                    prev_words.append(index_obj)
                    continue
                
            slist[row].append([ index_obj ])
    print('done processing matrix')

    lines = list(slist.keys())
    lines.sort()

    debug = 'Step 1\n'

    for line in lines:
        words_list = slist[line]
        temp = []
        for words in words_list:
            sentence = [ data[index]['description'] for index in words ]
            temp.append(' '.join(sentence))
        debug += str(line) + ' ' + str(temp) + '\n'
    debug += '\n'

    ssum = {}
    sres = {}

    for line in lines:
        words_list = slist[line]
        phrases = []
        for words in words_list:
            phrase = [ data[index]['description'] for index in words ]
            phrases.append(' '.join(phrase))
        ssum[line] = phrases
        sres[line] = phrases.copy() 
    def request_word(word):
        res = []
        try:
            r = requests.post(URL, data=word.encode('utf-8'), headers={'Content-Type': 'application/pdf'})
            r = r.json()
            
            for obj in r:
                if LABEL in obj:
                    res.append(obj[LABEL][0]['@id'])
            return res
        except Exception as e:
            print(word)
            print(e)
            return res
        
    def request_label(ssum, line, index, sres):
        res = request_word(ssum[line][index])
        sres[line][index] = res
        
    def request_line(ssum, line, sres):
        threads = [ threading.Thread(target=request_label, args=(ssum, line, index, sres)) for index in range(len(ssum[line])) ]
        for thread in threads:
            thread.start()
            
        for thread in threads:
            thread.join()
            
    def request_all(ssum, sres):
        threads = [ threading.Thread(target=request_line, args=(ssum, line, sres)) for line in ssum.keys() ]
        for thread in threads:
            thread.start()
            
        for thread in threads:
            thread.join()
        
    print('requesting...')
    request_all(ssum, sres)
    print('finish requesting...')

    debug += 'Step 2\n'
    for line, words in sres.items():
        debug += str(line) + ' ' + str(words) + '\n'
    debug += '\n'


    def concat(ssum, line, index):                            
        paragraph = ''                                         
        for l, phrases in ssum.items():                         
            if l < line:                                       
                continue
            if l == line:
                paragraph += ' '.join(phrases[index:])
            else:
                sentence = ' '.join(phrases)
                paragraph += '\n' + sentence
        return paragraph

    def strip(s):
        return ''.join(re.split('[^a-zA-Z0-9]', s.lower()))

    # processing text
    date_regex = '\d{1,2}\s?[\.\-\/\:]\s?\d{1,2}\s?[\.\-\/\:]\s?\d{2,4}|\d{1,2}\s?\-?\s?[A-Za-z]{3,}\s?\-?\s?(\d{2}|\d{4})'
    number_regex = '(\,? ?\d+ ?)+(\. ?\d+)?(?!\d{0,} ?%)'
    percentage_regex = '\d+\s?%|\d+\s?\.\s?%'

    # processing text
    patterns = {                                               
        'pan': {                                               
            'regex': '[A-Z]{5}[0-9]{4}[A-Z]{1}',               
            'label': 'PAN_NO',
            'main_key': 'PAN_NO',
        },
        'tan': {                                               
            'regex': '[A-Z]{4}[0-9]{5}[A-Z]{1}',
            'label': 'TAN',
            'main_key': 'TAN',
        },
        'purchase_number': {                                   
            'regex': '\d{1,}',
            'label': 'Purchase_Order_Number',
            'main_key': 'Purchase_Order_Number',
        },
        'gstin': {
            'regex': '[A-Z0-9]{15}',
            'label': 'GSTIN',
            'main_key': 'GSTIN',
        },
        'tan': {
            'regex': '[A-Z]{4}[0-9][4]{A-Z}{1}',
            'label': 'TAN',
            'main_key': 'TAN',
        },
        'invoice_no': {
            'regex': '\d+|[A-Z]+\d+[A-Z]\s?(\d{2})?\s?\-\s?\d+|[A-Z]\s?\-?\s?\d+',
            'label': 'INVOICE_NO',
            'main_key': 'INVOICE_NO',
        },
        # 12345 | SA18Y 09 - 00205 | S - 168
        'invoice_date': {
            'regex': date_regex,
            'label': 'INVOICE_NO',
            'main_key': 'Invoice_Date',
        },
        'invoice_date_1': {
            'regex': date_regex,
            'label': 'Invoice_Date',
            'main_key': 'Invoice_Date',
        },
        # (MM.DD.YYYY) | MM - DDD - YYYY | MM -/ DD -/ YYYY
        'lrno': {
            'regex': '\d{1,2}\s?[(th)\-\.]?\s?([A-Za-z]{3,})\s?[\-\.]?\s?\d{4}|\d{1,2}\s?\.\s?\d{1,2}\s?\.\s?\d{2}|BY MONTH\s?\/?\s?\d{4}\s?\-\s?\d{4}',
            'label': 'L.R._No',
            'main_key': 'L.R._No',
        },
        'lrdt': {
            'regex': date_regex,
            'label': 'L.R._Dt',
            'main_key': 'L.R._Dt',
        },

    }

    number_regex = '(\,? ?\d+ ?)+(\. ?\d+)?(?!\d{0,} ?%)'

    patterns_reversed = {
        'sgst': {
            'regex': number_regex,
            'label': 'SGST',
            'main_key': 'SGST_Amount',
        },
        'cgst': {
            'regex': number_regex,
            'label': 'CGST',
            'main_key': 'CGST_Amount',
        },
        'igst': {
            'regex': number_regex,
            #1 93 , 626 . 00
            'label': 'IGST',
            'main_key': 'IGST_Amount',
        },
        'sgst_percentage': {
            'regex': percentage_regex,
            'label': 'SGST',
            'main_key': 'SGSTPercentage',
        },
        'cgst_percentage': {
            'regex': percentage_regex,
            'label': 'CST',
            'main_key': 'CSTPercentage',
        },
        'igst_percentage': {
            'regex': percentage_regex,
            'label': 'IST',
            'main_key': 'ISTPercentage',
        },
        'total': {
            'regex': number_regex,
            'label': 'Gross_Total',
            'main_key': 'Gross_Total',
        },
    }
    result_keys = [
        'Tax_Invoice',
        'INVOICE_NO',
        'Ship_To',
        'Bill_To',
        'EssarGSTIN',
        'Hazira',
        'Invoice_Date',
        'PAN_NO',
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
        'Gross_Total',
    ]

    number_keys = [
        'SGST_Amount',
        'CGST_Amount',
        'IGST_Amount',
        'Gross_Total',
    ]

    result = { 'items': [] }
    for key in result_keys:
        result[SELECTED_LABEL + key] = None

    for line, phrases in ssum.items():
        for index, phrase in enumerate(phrases):
            lookup = sres[line][index]
            if not isinstance(lookup, list):
                continue

            result_label = [ strip(label.split('#')[-1]) for label in result.keys() if result[label] is not None ]    
            labels = [ strip(word.split('#')[-1]) for word in lookup ]

            if 'taxinvoice' in labels and 'taxinvoice' not in result_label:
                taxinvoice_index = labels.index('taxinvoice')
                result[lookup[taxinvoice_index]] = True

            for pattern in patterns.values():
                # ignore already exist key in result
                if strip(pattern['main_key']) in result_label:
                    continue

                if strip(pattern['label']) in labels:
                    print(line, pattern['main_key'])
                    # concatenate the remaining text
                    label_index = labels.index(strip(pattern['label']))
                    paragraph = concat(ssum, line, index)
                    search_p = re.search(pattern['regex'], paragraph)
                    if search_p is not None:
                        result[SELECTED_LABEL + pattern['main_key']] = search_p.group(0)

    for line in lines[::-1]:
        phrases = ssum[line]
        for index in range(len(phrases) - 1, -1, -1):
            phrase = phrases[index]
            lookup = sres[line][index]
            if not isinstance(lookup, list):
                continue

            result_label = [ strip(label.split('#')[-1]) for label in result.keys() if result[label] is not None ]    
            labels = [ strip(word.split('#')[-1]) for word in lookup ]

            for pattern in patterns_reversed.values():
                # ignore already exist key in result
                if strip(pattern['main_key']) in result_label:
                    continue

                if strip(pattern['label']) in labels:
                    print(line, pattern['main_key'])
                    # concatenate the remaining text
                    label_index = labels.index(strip(pattern['label']))
                    paragraph = concat(ssum, line, index)
                    search_p = re.search(pattern['regex'], paragraph)
                    if search_p is not None:
                        key = SELECTED_LABEL + pattern['main_key']
                        result[key] = search_p.group(0)
                        if pattern['main_key'] in number_keys:
                            result[key] = to_float(result[key])

    debug += 'Step 3\n'
    debug += json.dumps(result, indent=2)
    
    return result, debug

            
if __name__ == '__main__':
    r = process_json('p4materials/c20.json')
    print(r)
    # r1 = process_json('p4materials/c20/c20.json')
    # r2 = process_json('p4materials/c21/c21.json')
    # r3 = process_json('p4materials/c22/c22.json')

    # print('Image 1')
    # print(json.dumps(r1, indent=2))
    # print()

    # print('Image 2')
    # print(json.dumps(r2, indent=2))
    # print()

    # print('Image 3')
    # print(json.dumps(r3, indent=2))
    # print()

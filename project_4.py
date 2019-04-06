import json
import requests
import threading
import itertools
import re


A = 1000000
MARGIN_Y = 8
MARGIN_X = 5
MARGIN_MERGE_Y = 2
MARGIN_MERGE_X = 6
URL = 'http://35.188.227.39:8080/enhancer/chain/ReceiptChain'
LABEL = 'http://fise.iks-project.eu/ontology/entity-reference'
SELECTED_LABEL = 'http://bizlem.io'
IGNORES = [
    'DESCRIPTION_OF_SERVICES',
    'HSN%2FSAC',
    'Unit',
    'Quantity',
    'RATE',
    'AMOUNT',
]


def middle(value, left, right):
    return value >= left and value <= right

def get_x(pos, index=0):
    return pos['bounding_poly']['vertices'][index]['x']

def get_y(pos, index=0):
    return pos['bounding_poly']['vertices'][index]['y']

def get_box(pos):
    return [
        get_x(pos),
        get_y(pos),
        get_x(pos, 2),
        get_y(pos, 2),
    ]

def update_box(box, p):
    box[0] = min(box[0], p[0])
    box[1] = min(box[1], p[1])
    box[2] = max(box[2], p[2])
    box[3] = max(box[3], p[3])

def can_merge(box1, box2):
    return middle(box2[1], box1[3] - MARGIN_MERGE_Y, box1[3] + MARGIN_MERGE_Y) and middle(box2[0], box1[0] - MARGIN_MERGE_X, box1[2] + MARGIN_MERGE_X)

def strip(s):
    return ''.join(re.split('[^a-zA-Z0-9]', s.lower()))

def request_title(slist, line, index, sresult):
    try:
        r = requests.post(URL, data=slist[line][index].encode('utf-8'), headers={'Content-Type': 'application/pdf'})
        res = r.json()
    except e:
        print(slist[line][index], line, index)
        print(e)

    for obj in res:
        if LABEL in obj:
            if isinstance(sresult[line][index], str):
                sresult[line][index] = []

            sresult[line][index].append(obj[LABEL][0]['@id'])

def format_result(result):
    return result

    
def process_text(slist, sresult, sizes):
    # find item list
    # find header title
    result = { 'items': [] }
    header_index = None
    header_length = 0

    print('find header title')

    for line, value in slist.items(): 
        if len(value) >= header_length:
            found_url = False
            for v in sresult[line]:
                if isinstance(v, list):
                    for word in v: 
                        if SELECTED_LABEL in word:
                            found_url = True
                            break
                    if found_url:
                        break

            if not found_url:
                continue

            header_index = line
            header_length = len(value)

    headers = []
    for v in sresult[header_index]:
        if isinstance(v, list):
            headers.append(v[0])
        else:
            headers.append(v)

    header_lookup = []
    header_size = sizes[header_index]

    for i in range(len(header_size)):
        if i > 0:
            left = header_size[i - 1][2]
        else: 
            left = 0

        if i < len(header_size) - 1:
            right = header_size[i + 1][0]
        else:
            right = A

        header_lookup.append((left, right)) 

    items = []
    if header_index is not None:
        meet_table = False

        for line, value in slist.items():
            if line <= header_index:
                continue

            if not meet_table and len(value) < len(headers) - 2:
                continue

            if meet_table and len(value) < len(headers) - 2:
                break
            
            obj = {}
            meet_table = True

            print(header_lookup)
            for index, v in enumerate(value):
                for i, margin in enumerate(header_lookup):
                    if middle(sizes[line][index][0], margin[0], margin[1]) and middle(sizes[line][index][2], margin[0], margin[1]):
                        if SELECTED_LABEL not in headers[i]:
                            break

                        if headers[i] not in obj:
                            obj[headers[i]] = v
                        else:
                            obj[headers[i]] += ' ' + v
                        break

            items.append(obj)
    
    result['items'] = items

    patterns = {
        'pan': {
            'regex': '[A-Z]{5}[0-9]{4}[A-Z]{1}',
            'label': 'PAN_NO'
        },
        'tan': {
            'regex': '[A-Z]{4}[0-9]{5}[A-Z]{1}',
            'label': 'TAN',
        },
        'purchase_number': {
            'regex': '\d{1,}',
            'label': 'Purchase_Order_Number',
        },
        'gstin': {
            'regex': '[A-Z0-9]{15}',
            'label': 'GSTIN',
        },
    }

    def concat(slist, line, index):
        paragraph = ''
        for l, words in slist.items():
            if l < line:
                continue
            if l == line:
                paragraph += ' '.join(slist[line][index:])
            else:
                sentence = ' '.join(slist[line])
                paragraph += '\n' + sentence
        return paragraph

    for line, words in slist.items():
        for index, word in enumerate(words):
            lookup = sresult[line][index]
            if isinstance(lookup, list):
                labels = [ strip(l.split('#')[-1]) for l in lookup ]
                if 'taxinvoice' in labels:
                    taxinvoice_index = labels.index('taxinvoice')
                    result[lookup[taxinvoice_index]] = 'yes'

                for pattern in patterns.values():
                    result_label = [ strip(k.split('#')[-1]) for k in result.keys() ]
                    # ignore already exist key in result
                    if strip(pattern['label']) in result_label:
                        continue

                    if strip(pattern['label']) in labels:
                        # concatenate the remaining text
                        label_index = labels.index(strip(pattern['label']))
                        paragraph = concat(slist, line, index)
                        search_p = re.search(pattern['regex'], paragraph)
                        if search_p is not None:
                            result[lookup[label_index]] = search_p.group(0)


    '''
    sflat = list(itertools.chain.from_iterable(slist.values()))
    sresult_flat = list(itertools.chain.from_iterable(sresult.values()))
    sflat = [ v for v in sflat if len(v) > 0 ]
    sresult_flat = [ v for v in sresult_flat if len(v) > 0 ]

    i = 0
    while i < len(sflat):
        if isinstance(sresult_flat[i], list):
            is_item = False
            for key in sresult_flat[i]:
                v = key.split('#')
                if v[-1] in IGNORES:
                    is_item = True
                    break

            if is_item or i == len(sflat) - 1:
                i += 1
                continue

            length = len(sflat[i + 1]) // len(sresult_flat[i])
            value = []            
            for j in range(0, len(sresult_flat[i])) :
                if j == len(sresult_flat[i]) - 1:
                    value.append(sflat[i + 1][j * length:])
                else:
                    value.append(sflat[i + 1][(j * length):((j + 1) * length)])

            for j, word in enumerate(sresult_flat[i]):
                result[word] = value[j]

        i += 1
    '''

    return result


def process_json(path):
    with open(path) as f:
        data = json.load(f)

    # Build histogram
    y = [ get_y(p) for p in data ]
    y.sort()

    cur_y = y[0]
    hist_y = { cur_y: cur_y }

    for yc in y:
        if yc == cur_y: 
            continue
        if yc - cur_y > MARGIN_Y:
            cur_y = yc
        hist_y[yc] = cur_y

    
    # sort by y, then by x
    def compare_pos(pos):
        return hist_y[get_y(pos)] * A + get_x(pos) 

    data.sort(key=compare_pos)

    slist = {}
    sizes = {}

    for i in range(1, len(data)):
        hy = hist_y[get_y(data[i])]
        if hy not in slist:
            slist[hy] = [ [ data[i] ] ]
            sizes[hy] = [ get_box(data[i]) ]
        else:
            if get_x(data[i]) - sizes[hy][-1][2] <= MARGIN_X:
                slist[hy][-1].append(data[i]) 
                update_box(sizes[hy][-1], get_box(data[i]))
            else:
                slist[hy].append([ data[i] ])
                sizes[hy].append(get_box(data[i]))
    
    keys = list(slist.keys())

    for i in range(1, len(keys)):
        removed = []

        for jb, box in enumerate(sizes[keys[i]]):
            for jub, upper_box in enumerate(sizes[keys[i - 1]]):
                if can_merge(upper_box, box):
                    slist[keys[i - 1]][jub] += slist[keys[i]][jb]
                    update_box(upper_box, box)
                    removed.append(jb)
                    break

        for jb in removed[::-1]:
            sizes[keys[i]].pop(jb)
            slist[keys[i]].pop(jb)

    temp_slist = {}
    for k, v in slist.items():
        if len(v) == 0:
            continue

        value = []
        for p in v:
            s = ' '.join([ pb['description'] for pb in p ])
            value += s.split(':')

        temp_slist[k] = value
    
    slist = temp_slist

    # requesting
    sresult = { k: v.copy() for k, v in slist.items() }
    threads = []

    print('requesting...')
    for line, value in slist.items():
        for i, word in enumerate(value):
            threads.append(threading.Thread(target=request_title, args=(slist, line, i, sresult)))

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()
   
    print(data[0]['description'])
    for k, v in slist.items():
        print(k, v)
        print(k, sresult[k])
    input()

    result = process_text(slist, sresult, sizes)
    format_result(result)

    print(json.dumps(result, indent=2))

    return result


if __name__ == '__main__':
    r = process_json('p4materials/c10/c10.json')


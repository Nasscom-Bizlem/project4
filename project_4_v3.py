import json
import requests
import threading
import re

URL = 'http://3.84.244.86:8080/enhancer/chain/PurchaseOrder'
LABEL = 'http://fise.iks-project.eu/ontology/entity-reference'
TEXT_LABEL = 'http://fise.iks-project.eu/ontology/selected-text'
RELATION_LABEL = 'http://purl.org/dc/terms/relation'
SELECTED_LABEL = 'http://bizlem.io/PurchaseOrderProcessing#'

def middle(value, left, right):
    return value >= left and value <= righft

def get_x(obj, index=0):
    return obj['bounding_poly']['vertices'][index]['x']

def get_y(obj, index=0):
    return obj['bounding_poly']['vertices'][index]['y']

def to_float(s):
    s_format = ''.join(re.split('[^\d\.]', s))
    return float(s_format)

def strip(s):
    return ''.join(re.split('[^a-zA-Z0-9]', s.lower()))


def parseJSON(data, x_thres, y_thres,
    word_special_chars=['+', '-', ';', '-', '/', '\\', "'", '"'],
    number_special_chars=[',', '.'],
):
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


        if yMid <= curLineYCoord + y_thres and yMid >= curLineYCoord - y_thres:
            if xStart <= curXEnd + x_thres \
                or (curWord[0] in characters) \
                or curWord[0] in close_chars \
                or ((curWord[0] in set(number_special_chars)) # need char % \
                    and '0' <= completedWord[-1] and completedWord[-1] <= '9') \
                or completedWord[-1] in characters \
                or completedWord[-1] in open_chars \
                or (len(completedWord) >= 2 and completedWord[-1] in set(number_special_chars) \
                    and '0' <= curWord[0] and curWord[0] <= '9' \
                    and '0' <= completedWord[-2] and completedWord[-2] <= '9'):
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

    return response

def p4_process_json(path,
        verbose=True,
        url_api='http://3.84.244.86:8080/enhancer/chain/PurchaseOrder',
        x_thresh=0,
        y_thresh=10,
        word_special_chars=[],
        number_special_chars=[]
    ):

    URL = url_api

    with open(path) as f:
        data = json.load(f)
        data_sum = data[0]
        data = data[1:]

    slist = parseJSON(data, x_thresh, y_thresh,
        word_special_chars=word_special_chars,
        number_special_chars=number_special_chars,
    )
    # slist = data['lines']

    max_y = slist[-1]['y']
    max_x = 0
    for line in slist:
        if len(line['words']) == 0:
            continue

        last_word = line['words'][-1]['x2']
        if last_word > max_x:
            max_x = last_word


    # for line in slist:
    #     print(line['y'], [ word['word'] for word in line['words'] ])
    # return

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
            r = requests.post(URL, data=sentence.encode('utf-8'), headers={'Content-Type': 'application/pdf'})
            r = r.json()

            for obj in r:
                if LABEL in obj and RELATION_LABEL in obj:
                    res[obj[LABEL][0]['@id']] = obj[RELATION_LABEL][0]['@id']

            for obj in r:
                if TEXT_LABEL in obj and '@id' in obj:
                    obj_id = obj['@id']
                    for kurl, vid in res.items():
                        if vid == obj_id:
                            res[kurl] = obj[TEXT_LABEL][0]['@value']

            current_url = None
            current_word_indexes = []
            merging_r = {}

            word_length = len(slist[line_index]['words'])
            for word_index, word in enumerate(slist[line_index]['words']):
    #             print(word_index, word['word'])
                for kurl, vtext in res.items():
                    if kurl in merging_r:
                        continue

                    if word['word'] in vtext or vtext in word['word']:
                        if kurl == current_url or current_url is None:
                            current_word_indexes.append(word_index)
                        elif kurl != current_url:
                            merging_r[current_url] = current_word_indexes
                            current_word_indexes = [ word_index ]

                        current_url = kurl

                        break

            if current_url is not None and current_url not in merging_r:
                merging_r[current_url] = current_word_indexes


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
            sres_words[line_index]['words'] = sres_words_newarr

            sres[line_index]['words'] = list(res.keys())
    #         print(sres_words_newarr)

        except Exception as e:
            print(line_index, sentence, 'error')
            print(e)
            return res


    def request_all(sres):
        threads = [ threading.Thread(target=request_line_and_replace, args=(slist, sres, sres_words, line_index)) for line_index in range(len(sres)) ]
        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()

    if verbose:
        print('requesting...')
    request_all(sres)
    if verbose:
        print('finish requesting...')

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
                    'url': None,
                })
            else:
                words.append({
                    'word': word['word'],
                    'x1': word['x1'],
                    'x2': word['x2'],
                    'url': url,
                })
        debug.append({
            'y': line['y'],
            'line_index': line_index,
            'number_of_words': len(words),
            'words': words,
        })


    def concat(slist, line_index, index):
        paragraph = ' '.join([ word['word'] for word in slist[line_index]['words'][index:] ]) + '\n'
        current_word = slist[line_index]['words'][index]

        for line in slist[line_index + 1:line_index + 3]:
            for word in line['words']:
                if word['x1'] >= current_word['x1'] - 15:
                    paragraph += word['word'] + ' '
            paragraph += '\n'

        return paragraph


    date_regex = '\d{1,2}\s?[\.\-\/\:]\s?\d{1,2}\s?[\.\-\/\:]\s?\d{4}|\d{1,2}\s?\-?\s?[A-Za-z]{3,}\s?\-?\s?\d{4}|\d{1,2}\s?\/\s?\d{1,2}\s?\/\s?\d{2}'
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

    result = { }
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

                if verbose:
                    print(line_index, line['y'], pattern['main_key'])

                search_p = re.search(pattern['regex'], paragraph)
                if search_p is not None:
                    result[SELECTED_LABEL + pattern['main_key']] = search_p.group(0)

    for key in number_keys:
        if result[SELECTED_LABEL + key] is not None:
            result[SELECTED_LABEL + key] = to_float(result[SELECTED_LABEL + key])


    header_keys = set([
        'SNo',
        'DescriptionOfServices',
        'HSN/SAC',
        'Unit',
        'Quantity',
        'Rate',
        'AmountNumbers',
    ])

    def createItem():
        item = {}
        for key in header_keys:
            item[SELECTED_LABEL + key] = None
        return item

    selected = []
    header_map = {}

    stop_line_index = 0
    end = False

    for line_index, words in enumerate(slist):
        if len(sres[line_index]['words']) == 0: continue

        for word_index, word in enumerate(words['words']):
            url = sres_words[line_index]['words'][word_index]
            if SELECTED_LABEL not in url: continue

            url_key = url.split('#')[-1]
            if url_key == 'GrossTotal':
                stop_line_index = line_index
                end = True
                break

            if url_key not in header_keys: continue

            x1, x2 = word['x1'], word['x2']

            header_map.setdefault(SELECTED_LABEL + url_key, [])
            header_map[SELECTED_LABEL + url_key].append((line_index, words['y'], x1, x2))

        if end:
            break

    freq = [0] * max_y

    for url_key, pos_arr in header_map.items():
        for _, y, _, _ in pos_arr:
            start_y = max(0, y - 30)
            stop_y = min(max_y, y + 30)
            for i in range(start_y, stop_y + 1):
                freq[i] += 1

    max_vote = len(header_map.keys())
    deleted_url = []
    for url_key, pos_arr in header_map.items():
        selected_pos = None
        for pos_index, (_, y, _, _) in enumerate(pos_arr):
            if freq[y] >= max_vote - 1:
                selected_pos = pos_index
                break

        if selected_pos is not None:
            header_map[url_key] = pos_arr[selected_pos]
        else:
            deleted_url.append(url_key)

    for url_key in deleted_url:
        del header_map[url_key]

    if verbose:
        print(header_map)

    # add info to header_info
    header_info = {}
    header_info['positions'] = []
    for _, (line_index, y, _, _) in header_map.items():
        new_item = True
        for h in header_info['positions']:
            if h['y'] == y:
                new_item = False
                break

        if new_item:
            header_info['positions'].append({
                'y': y,
                'line_index': line_index,
            })
    # header_temp = [ (header_key, line_index, y, x1, x2) for header_key, (line_index, y, x1, x2) in header_map.items() ]
    # header_temp.sort(key=lambda x: x[3])
    # for header_key, line_index, y, x1, x2 in header_temp:
    #     header_info['positions'][header_key] = {
    #         'line_index': line_index,
    #         'y': y,
    #         'x1': x1,
    #         'x2': x2,
    #     }


    header_pos = []
    start_line_index = len(slist)

    for header, (line_index, y, left_x, right_x) in header_map.items():
        header_pos.append((header, y, [left_x, right_x]))
        if start_line_index > line_index:
            start_line_index = line_index

    header_pos.sort(key=lambda x: x[-1][0])
    for header, y, x_pos in header_pos:
        if 'DescriptionOfServices' not in header:
            x_pos[0] -= 30
            x_pos[1] += 30
        else:
            x_pos[0] -= 200
            x_pos[1] += 200

    header_pos[-1][-1][-1] = max_x

    if verbose:
        print(header_pos)


    # add info to header info
    # header_info['lines'] = []
    # for i in range(start_line_index, stop_line_index):
    #     header_info['lines'].append(sres_words[i])

    items = []
    item = createItem()

    header_check = {}
    for header, _, _ in header_pos:
        header_check[header] = None

    for words_line, words in enumerate(slist[start_line_index + 1:stop_line_index]):
        for word in words['words']:
            found = False
            x1, x2 = word['x1'], word['x2']
            for header, header_line, pos in header_pos:
                if x1 >= pos[0] and x2 <= pos[1]:
                    desc = word['word']
                    found = header
                    if item[header] is None:
                        item[header] = desc

                    if 'DescriptionOfServices' not in header:
                        if header_check[header] is None:
                            header_check[header] = words_line
                        elif header_check[header] != words_line:
                            items.append(item)
                            item = createItem()
                            for h in header_check.keys():
                                header_check[h] = None
                        else:
                            item[header] += ' ' + desc
                    else:
                        item[header] += ' ' + desc
                    break

            if verbose:
                print(words['y'], word['word'], x1, x2, found)

    if len([ value for value in item.values() if value is not None ]) > 0:
        items.append(item)

    result['items'] = items


    return result, debug, header_info


if __name__ == '__main__':
    r, debug, header_info = p4_process_json('p4materials/c3.json', verbose=True)
    print(r)
    print(debug)
    print(header_info)
    print(json.dumps(r, indent=2))


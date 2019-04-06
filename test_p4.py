import json
import os

from project_4_v3 import process_json


with open('p4materials/test_data.json') as f:
    test_data = json.load(f)


files = test_data.keys()
print('Total', len(files), files)

for i, f in enumerate(files):
    print('Processing file ', f, '(%r/%r)' % (i + 1, len(files)))
    output, _ = process_json(os.path.join('p4materials', f + '.json'), verbose=False)

    diff = 0
    for key in output.keys():
        if key == 'items':
            continue
            
        if key not in test_data[f]:
            diff += 1
            print('\t', key, 'not found in test data')
        elif output[key] != test_data[f][key]:
            diff += 1
            print('\t', key, output[key], test_data[f][key])
        
    if diff == 0:
        print('\tGood content')

print('Done!')


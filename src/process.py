from __future__ import print_function
import glob
import hashlib

import pandas as pd

def parse_text_file(fn):
    record_type = None
    record_count = 1
    source = fn.rstrip(".txt").replace("data/", "")
    
    with open(fn) as f:
        it = iter(f)
        record = None
        
        try:
            while True:
                line = it.next().strip()
                if line == "":
                    record_type = None
                    continue
                if line == "---":
                    record_count += 1
                    continue
                if ":" not in line:
                    record_type = line
                    continue
                if line.startswith("source:"):
                    continue
                if line.startswith("note:"):
                    continue
                line = "%s-%s" % (record_type, line)
                record = {'record-type': line.split("-")[0],
                          'source': source+"/"+str(record_count)}
                record[",".join(line.split(":")[0].split("-")[1:])] = line.split(":")[1].strip()
                while line != "":
                    line = it.next().strip()
                    if line == "": break
                    line = "%s-%s" % (record_type, line)
                    record[",".join(line.split(":")[0].split("-")[1:])] = line.split(":")[1].strip()
                yield record
                record = None
        except StopIteration:
            if record is not None:
                yield record
                record = None

def parse_excel_file_blau(fn):
    source = fn.split(".")[0].replace("data/", "")
    dflist = []
    df = pd.read_excel(fn)
    df.rename(inplace=True,
              columns={'person.ref':        'person',
                       'person.name.last':  'name.last',
                       'person.name.first': 'name.first',
                       'person.name.given': 'name.given',
                       'person.name.nickname':  'name.nick',
                       'team.name':         'club',
                       'league.name':       'league'
                       })
    df['tmp'] = range(10000, 10000+len(df))
    df['person'] = df['person'].fillna(df['tmp'])
    df['person'] = df['person'].apply(lambda x: "%d" % x)
    for col in df.columns:
        if col.startswith("name.") or col.startswith("birth.") or \
           col.startswith("death."):
            df[col] = df.groupby('person')[col].transform('last')
    for record_type in ['name']:
        # TODO: birth and death - should drop all null records
        subdf = df[['person'] + 
                   [c for c in df.columns
                    if c.startswith(record_type+".")]].copy()
        for col in subdf.columns:
            if col.startswith(record_type+"."):
                subdf.rename(inplace=True, columns={col: col.split(".", 1)[1]})
        subdf['record-type'] = record_type
        dflist.append(subdf.drop_duplicates())

    df = df[['type', 'person', 'date', 'club', 'league']].copy()
    df['league'] = df['league'].str.replace(" League", "")
    df['date'] = df['date'].astype(str).str.split(".").str[0]
    df['record-type'] = 'affiliation'
    dflist.append(df)
    
    df = pd.concat(dflist, ignore_index=True)
    df['source'] = source
    return df


                       

                
def parse_excel_file(fn):
    source = fn.split(".")[0].replace("data/", "")
    dflist = []
    try:
        df = pd.read_excel(fn, sheet_name="Subjects")
    except Exception as e:
        if "No sheet named" in str(e):
            # Probably a Blau-format file
            return parse_excel_file_blau(fn)
        raise
    
    df.rename(inplace=True,
              columns={'bats':     'meta.bats',
                       'throws':   'meta.throws',
                       'height':   'meta.height',
                       'weight':   'meta.weight',
                       'by':       'birth.year',
                       'bm':       'birth.month',
                       'bd':       'birth.day',
                       'bc':       'birth.country',
                       'bs':       'birth.state',
                       'bt':       'birth.city',
                       'dy':       'death.year',
                       'dm':       'death.month',
                       'dd':       'death.day',
                       'dc':       'death.country',
                       'ds':       'death.state',
                       'dt':       'death.city'})
    for record_type in ['name', 'meta', 'birth', 'death']:
        subdf = df[['person'] + 
                   [c for c in df.columns
                    if c.startswith(record_type+".")]].copy()
        for col in subdf.columns:
            if col.startswith(record_type+"."):
                subdf.rename(inplace=True, columns={col: col.split(".", 1)[1]})
        subdf['record-type'] = record_type
        dflist.append(subdf)

    df = pd.read_excel(fn, sheet_name="Playing")
    df = df[['person', 'date', 'club', 'league', 'position']]
    df['record-type'] = "affiliation"
    df['type'] = "playing"
    dflist.append(df)

    try:
        df = pd.read_excel(fn, sheet_name="Managing")
        df = df[['person', 'date', 'club', 'league']]
        df['record-type'] = "affiliation"
        df['type'] = "managing"
    except Exception as e:
        if "No sheet named" not in str(e):
            raise
    
    df = pd.concat(dflist, ignore_index=True)
    df['source'] = source
    return df
    

                
def int_to_base(n):
    alphabet = "BCDFGHJKLMNPQRSTVWXYZ"
    base = len(alphabet)
    if n < base:
        return alphabet[n]
    else:
        return int_to_base(n // base) + alphabet[n % base]
                
def hash_djb2(s):
    hash = 5381
    for x in s:
        hash = ((hash << 5) + hash) + ord(x)
    return int_to_base(hash)[-7:]

def generate_hash(s):
    return int_to_base(int(hashlib.sha1(s).hexdigest(), 16))[-7:]

                
def generate_clubs():
    names = pd.read_csv("processed/name.csv", dtype=str)
    affiliations = pd.read_csv("processed/affiliation.csv", dtype=str)

    clubs = pd.merge(affiliations, names, how='left',
                     on=['source', 'person'])
    if 'caliber' in clubs:
        clubs = clubs[clubs['caliber'].isnull() |
                     (clubs['caliber']=='professional')]
    clubs = clubs[~clubs['date'].isnull()]
    clubs = clubs[~clubs['league'].isnull()]
    clubs = clubs[~clubs['club'].isnull()].copy()
    clubs['league'] = clubs['league'].apply(lambda x: x+" League"
                                            if "Association" not in x and "League" not in x
                                            else x)
    clubs['person'] = clubs['source'].str.split('/').str[0] + '/' + \
                      (clubs['source']+"/"+clubs['person']).apply(generate_hash)
    clubs['first'] = clubs['first'].fillna(clubs['given'])
    clubs['date'] = clubs['date'].str.split('.').str[0]

    clubs['league'] = clubs['league'].replace({'I-I-I League': 'Illinois-Indiana-Iowa League',
                                               'K-I-T League': 'Kentucky-Illinois-Tennessee League',
                                               'M-I-N-K League': 'Minnesota-Iowa-Nebraska-Kansas League',
                                               'MINK League': 'Minnesota-Iowa-Nebraska-Kansas League',
                                               'Mink League': 'Minnesota-Iowa-Nebraska-Kansas League'})
    clubs.sort_values(['last', 'first', 'date', 'league', 'club'],
                      inplace=True)
    clubs.to_csv("processed/clubs.csv", index=False, encoding='utf-8')
    
                
def main():
    dflist = []

    for fn in glob.glob("data/morris/*/*.txt"):
        print(fn)
        dflist.append(pd.DataFrame(parse_text_file(fn)))
    for fn in glob.glob("data/*/*.xls"):
        print(fn)
        dflist.append(parse_excel_file(fn))
    print()
    
    df = pd.concat(dflist, ignore_index=True)
    if 'person' not in df:
        df['person'] = None
    df['person'] = df['person'].fillna('_subject')
    for (record_type, data) in df.groupby('record-type'):
        print("Writing %s records" % record_type)
        data = data.dropna(axis=1, how='all')
        del data['record-type']
        data.to_csv("processed/%s.csv" % record_type, index=False,
                    encoding='utf-8')

    generate_clubs()

        
            
if __name__ == '__main__':
    main()

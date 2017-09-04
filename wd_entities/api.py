"""
#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=#
  Ontology Engineering Group
        http://www.oeg-upm.net/
#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=#
  Copyright (C) 2016 Ontology Engineering Group.
#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=#
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

            http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=#
"""

import json
from StringIO import StringIO

import requests
from flask import Flask, request
from flask.json import jsonify
from flask_cache import Cache
from requests import Response
from urllib3 import HTTPResponse
from requests.structures import CaseInsensitiveDict
from requests.utils import get_encoding_from_headers

__author__ = 'Fernando Serena'

try:
    with open('o-properties.json') as f:
        wd_object_props = json.loads(f.read())
except:
    wd_object_props = {}

try:
    with open('d-properties.json') as f:
        wd_non_object_props = json.loads(f.read())
except:
    wd_non_object_props = {}

entity_labels = {}


def learn_prop(pr_id):
    if pr_id not in wd_non_object_props and pr_id not in wd_object_props:
        print 'learning {}...'.format(pr_id)

        wurl = 'https://www.wikidata.org/wiki/Special:EntityData/{}.json'.format(pr_id)
        response = requests.get(wurl)
        entity = response.json()
        prop = entity['entities'][pr_id]
        prop_datatype = prop['datatype']
        description = prop['descriptions'].get('en', {}).get('value', 'unknown')
        if prop_datatype == 'wikibase-item':
            super_props = prop['claims'].get('P1647', [])
            for sp in super_props:
                sp_id = sp['mainsnak']['datavalue']['value']['id']
                learn_prop(sp_id)
                if sp_id in wd_non_object_props:
                    wd_non_object_props[pr_id] = description
                    # break
            if pr_id not in wd_non_object_props:
                wd_object_props[pr_id] = description

            with open('o-properties.json', 'wb') as f:
                f.write(json.dumps(wd_object_props, indent=3))
        else:
            wd_non_object_props[pr_id] = description
            with open('d-properties.json', 'wb') as f:
                f.write(json.dumps(wd_non_object_props, indent=3))


def get_wd_entity_label(entity_id):
    if entity_id not in entity_labels:
        response = requests.get('https://www.wikidata.org/wiki/Special:EntityData/{}.json'.format(entity_id))
        entity = response.json()
        entity_labels[entity_id] = entity['entities'][entity_id]['labels']['en']['value']
    return entity_labels[entity_id]


def get_field_values(field):
    if field:
        if isinstance(field, dict):
            field = [field]
        for fd in field:
            fd = fd.get('en', {})
            if isinstance(fd, dict):
                fd = [fd]
            for langvalue in fd:
                yield langvalue.get('value', '')


def get_wd_entity(entity_id, pr_filter=()):
    # response = requests.get('http://www.wikidata.org/entity/' + entity_id)
    response = requests.get('https://www.wikidata.org/wiki/Special:EntityData/{}.json'.format(entity_id))
    d = {'entity': entity_id}

    if response.status_code == 200:
        data = response.json()

        entity = data['entities'][entity_id]
        claims = entity['claims']
        if entity['descriptions']:
            description = entity.get('descriptions', {}).get('en', {}).get('value', '')
            d['description'] = description
        d['aliases'] = list(get_field_values(entity['aliases']))
        d['labels'] = list(get_field_values(entity['labels']))
        for pr_id in claims:  # filter(lambda x: x in pr_filter, claims):
            # print 'processing property {}...'.format(pr_id)
            learn_prop(pr_id)
            for claim in claims[pr_id]:
                value = claim.get('mainsnak', {}).get('datavalue', {}).get('value', None)
                if value is None:
                    continue
                if pr_id == 'P18':
                    value = u'https://commons.wikimedia.org/wiki/File:{}'.format(value)
                elif pr_id == 'P373':
                    value = u'https://commons.wikimedia.org/wiki/Category:{}'.format(value)
                if isinstance(value, dict):
                    if value.get('entity-type') == 'item':
                        value = value.get('id')
                        if pr_id in wd_non_object_props:
                            value = get_wd_entity_label(value)
                    else:
                        value = value.get('text', value)
                if pr_id in d:
                    if not isinstance(d[pr_id], list):
                        d[pr_id] = [d[pr_id]]
                    d[pr_id].append(value)
                else:
                    d[pr_id] = value
            if isinstance(d.get(pr_id, None), set):
                d[pr_id] = list(d[pr_id])

    resp = HTTPResponse(
        status=response.status_code,
        body=StringIO(json.dumps(d)),
        headers=response.headers,
        preload_content=False,
    )
    response = Response()
    # Fallback to None if there's no status_code, for whatever reason.
    response.status_code = getattr(resp, 'status', None)
    # Make headers case-insensitive.
    response.headers = CaseInsensitiveDict(getattr(resp, 'headers', {}))
    # Set encoding.
    response.encoding = get_encoding_from_headers(response.headers)
    response._content = json.dumps(d)

    return response


app = Flask(__name__)
cache = Cache(app, config={'CACHE_TYPE': 'filesystem', 'CACHE_DIR': 'cache'})
mapped_properties = set.union(set(wd_non_object_props.keys()), set(wd_object_props.keys()))


def make_cache_key(*args, **kwargs):
    path = request.path
    return path.encode('utf-8')


@app.route('/entities/<qid>')
@cache.cached(timeout=3600, key_prefix=make_cache_key)
def get_entity(qid):
    response = get_wd_entity(qid, pr_filter=mapped_properties)
    return jsonify(response.json())


@app.route('/properties/<pid>')
@cache.cached(timeout=3600, key_prefix=make_cache_key)
def get_property(pid):
    if pid in wd_object_props:
        obj = True
        desc = wd_object_props.get(pid)
    elif pid in wd_non_object_props:
        obj = False
        desc = wd_non_object_props.get(pid)
    else:
        response = jsonify({'message': 'not found'})
        response.status_code = 404
        return response

    return jsonify({'object': obj, 'description': desc})


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5015, use_reloader=False, debug=False, threaded=True)

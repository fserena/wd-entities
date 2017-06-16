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

found_obj_props = {
    "P59": "the area of the celestial sphere of which the subject is a part (from a scientific standpoint, not an astrological one)",
    "P607": "battles, wars or engagements in which the person or item participated",
    "P135": "literary, artistic or philosophical movement associated with this person or work",
    "P136": "a creative work's genre or the genre in which an artist works. Use main subject (P921) to relate creative works to their topic",
    "P131": "the item is located on the territory of the following administrative entity. Use P276 (location) for specifying the location of non-administrative places and for items about events",
    "P527": "part of this subject. Inverse property of \"part of\" (P361).",
    "P1411": "award nomination received by a person, organisation or creative work (inspired from \"award received\" (Property:P166))",
    "P910": "main Wikimedia category (Category:xxxx)",
    "P1412": "language(s) that a person speaks or writes, including native languages",
    "P1416": "organization that a person is affiliated with",
    "P1303": "musical instrument that a person plays",
    "P166": "award or recognition received by a person, organisation or creative work",
    "P123": "organization responsible for publishing books, periodicals, games or software",
    "P1542": "effect of this cause",
    "P518": "part of the item for which the claim is valid",
    "P737": "this person, idea, etc. is informed by that other person, idea, etc., e.g. \"Heidegger was influenced by Aristotle\".",
    "P358": "link to discography in artist or band page",
    "P512": "academic degree that the person holds",
    "P511": "word or expression used before a name, in addressing or referring to a person",
    "P2283": "item or concept used by the subject or in the operation",
    "P1151": "Wikimedia portal associated with this topic",
    "P451": "someone with whom the person is in a relationship without being married. Use \"spouse\" for married couples.",
    "P1889": "item that is different from another item, but they are often confused",
    "P1884": "person's hair color. Use P585 as qualifier if there's more than one value.",
    "P119": "location of grave, resting place, place of ash-scattering, etc, (e.g. town/city or cemetery) for a person or animal. There may be several places: e.g. re-burials, cenotaphs, parts of body buried separately.",
    "P17": "sovereign state of this item",
    "P19": "most specific known (e.g. city instead of country, or hospital instead of city)",
    "P3373": "the subject has the object as their sibling (brother, sister, etc.). Use \"relative\" (P1038) for siblings-in-law (brother-in-law, sister-in-law, etc.) and step-siblings (step-brothers, step-sisters, etc.)",
    "P1066": "person who has taught this person",
    "P509": "underlying or immediate cause of death.  Underlying cause (e.g. car accident, stomach cancer) preferred.  Use 'manner of death' (P1196) for broadest category, e.g. natural causes, accident, homicide, suicide",
    "P184": "person who supervised the doctorate or PhD thesis of the subject",
    "P937": "location where persons were active",
    "P2354": "Wikimedia list related to this subject",
    "P463": "organization or club to which the subject belongs. Do not use for membership in ethnic or social groups, nor for holding a position such as a member of parliament (use P39 for that).",
    "P460": "this item is said to be the same as that item, but the statement is disputed",
    "P264": "brand and trademark associated with the marketing of subject music recordings and music videos",
    "P108": "organization for which the subject works or worked",
    "P106": "occupation of a person; see also \"field of work\" (Property:P101), \"position held\" (Property:P39)",
    "P101": "specialization of a person or organization, see P106 for the occupation",
    "P103": "language or languages a person has learned from birth",
    "P102": "the political party of which this politician is or has been a member",
    "P25": "female parent",
    "P27": "the object is a country that recognizes the subject as its citizen",
    "P26": "the subject has the object as their spouse (husband, wife, partner, etc.). Use \"partner\" (P451) for non-married companions",
    "P21": "sexual identity of subject: male (Q6581097), female (Q6581072), intersex (Q1097630), transgender female (Q1052281), transgender male (Q2449503). Animals: male animal (Q44148), female animal (Q43445). Groups of same gender use \"subclass of\" (P279)",
    "P20": "the most specific known (e.g. city instead of country, or hospital instead of city)",
    "P22": "male parent",
    "P3342": "person linked to the item in any possible way",
    "P1038": "family member (qualify with \"type of kinship\", P1039; for direct family member please use specific property)",
    "P641": "sport in which the entity participates or belongs to",
    "P800": "subject's notable scientific work or work of art, literature, or significance",
    "P802": "notable student(s) of a person",
    "P803": "professorship position held by this academic person",
    "P178": "organisation or person that developed this item",
    "P276": "location the item, physical object or event is within. In case of an administrative entity use P131. In case of a distinct terrain feature use P706.",
    "P172": "subject's ethnicity (consensus is that a VERY high standard of proof is needed for this field to be used. In general this means 1) the subject claims it him/herself, or 2) it is widely agreed on by scholars, or 3) is fictional and portrayed as such).",
    "P279": "all instances of these items are instances of those items; this item is a class (subset) of that item. Not to be confused with Property:P31 (instance of).",
    "P39": "subject currently or formerly holds the object position or public office",
    "P3448": "subject has the object as their stepparent",
    "P31": "that class of which this subject item is an individual example and member. Not to be confused with Property:P279 (subclass of).",
    "P1455": "link to the article with the works of a person",
    "P361": "object of which the subject is a part. Inverse property of \"has part\" (P527).",
    "P2439": "language associated with this item",
    "P1343": "dictionary, encyclopaedia, etc. where this item is described",
    "P1344": "event a person or an organization was a participant in, inverse of P710 or P1923",
    "P241": "branch to which this military unit, award, office, or person belongs, e.g. Royal Navy",
    "P161": "actor performing live for a camera or audience [use \"character role\" (P453) as qualifier] [use \"voice actor\" (P725) for voice-only role]",
    "P485": "the institution holding the subject's archives",
    "P1424": "primary infobox or navigational template of this subject",
    "P1429": "pet that a person owns",
    "P1853": "blood type of the (human or animal) item",
    "P40": "subject has the object in their family as their offspring son or daughter (independently of their age)",
    "P412": "person's voice type. expected values: soprano, mezzo-soprano, contralto, countertenor, tenor, baritone, bass (and derivatives)",
    "P1299": "object depicting this subject",
    "P410": "military rank achieved by a person (should usually have a \"start date\" qualifier)",
    "P417": "patron saint adopted by the subject",
    "P156": "immediately following item in some series of which the subject is part. Use P1366 (replaced by) if the item is replaced, e.g. political offices, states",
    "P155": "immediately prior item in some series of which the subject is part. Use P1365 (replaces) if the preceding item was replaced, e.g. political offices, states and there is no identity between precedent and following geographic unit",
    "P552": "handedness of the person",
    "P553": "a website that the person or organization has an account on (use with P554) Note: only used with reliable source or if the person or organization disclosed it.",
    "P551": "the place where the person is, or has been, resident",
    "P2959": "this item duplicates another item and the two can't be merged, as one Wikimedia project includes two pages, e. g. in different scripts or languages",
    "P452": "industry of company or organization",
    "P793": "significant or notable events associated with the subject",
    "P54": "sports teams or clubs that the subject currently represents or formerly represented",
    "P53": "include dynasty and nobility houses",
    "P3092": "member of the crew creating an audiovisual work, used for miscellaneous roles qualified with the job title when no specific property exists. Don't use if such a property is available: notably for cast member (P161), director (P57), etc.",
    "P2416": "discipline an athlete competed in within a sport",
    "P425": "activity corresponding to this occupation (use only for occupations - for people use Property:P101)",
    "P140": "religion of a person, organization or religious building, or associated with this subject",
    "P1875": "express a relationship between a subject and their agent",
    "P69": "educational institution attended by the subject",
    "P1050": "disease or other health problem affecting an individual human or other animal",
    "P1196": "circumstances of a person's death; one of: natural causes, accident, suicide, homicide, pending investigation or special 'unknown value'.  Use 'cause of death' (P509) for more immediate or underlying causes and events, e.g. heart attack, car accident",
    "P1962": "supporter or benefactor of the subject"
}

found_non_object_props = {
    "P2383": "identifier for a person in the directory of French learned societies",
    "P535": "identifier of an individual's  burial place in the Find a Grave database",
    "P2387": "identifier for a person, in the Elonet database",
    "P2267": "person or group whose statements have been fact checked by Politifact.com",
    "P434": "identifier for an artist per the MusicBrainz open music encyclopedia",
    "P2469": "identifier for a person, in the Theatricalia  database",
    "P1043": "identifier for an occupation on IDEO",
    "P1705": "label for the items in their official language (P37) or their original language (P364)",
    "P1048": "identifier for authority control issued by the National Central Library in Taiwan",
    "P2168": "identifier for a person on the Swedish Film Database (SFDb)",
    "P2167": "identifier for taxonomy of products and services for use in eCommerce",
    "P2163": "authority control identifier in WorldCat's \\u201cFAST Linked Data\\u201d authority file",
    "P1709": "equivalent class in other ontologies (use property URI)",
    "P2847": "Google+ account identifier of this person or organization: either starting with a \"+\" or consisting of 21 digits",
    "P1263": "identifier in the Notable Names Database, a biographical database: only for people entries",
    "P998": "Open Directory Project",
    "P2843": "identifier in Benezit Dictionary of Artists",
    "P443": "audio file with pronunciation",
    "P1814": "the reading of a Japanese name in kana",
    "P1816": "identifier for sitters and artists represented in the National Portrait Gallery, London",
    "P1543": "image of a person's monogram",
    "P1813": "short name of a place, organisation, person etc.",
    "P1819": "identifier for a person at genealogics.org",
    "P2688": "identifier of a person in the Box Office Mojo database",
    "P1015": "identifier in the Norwegian information system BIBSYS",
    "P1014": "identifier in the Art & Architecture Thesaurus by the Getty Research Institute",
    "P1017": "identifier for authority control used at the Vatican Library",
    "P2686": "identifier used by Opensecrets for people and organizations involved in US elections",
    "P3188": "ID in the Nobel prize organization homepage",
    "P213": "International Standard Name Identifier for an identity",
    "P1315": "identifier for people per National Library of Australia (see also P409 for the older Libraries Australia identifier)",
    "P214": "identifier in the Virtual International Authority File. Format: up to 22 digits",
    "P3265": "identifier for a person or organisation, at MySpace",
    "P1882": "identifier for an artist in the Web Gallery of Art",
    "P691": "identifier in the Czech National Authority Database (National Library of Czech Republic)",
    "P2042": "identifier at the artsy website",
    "P2041": "identifier assigned to an artist by the National Gallery of Victoria in Australia",
    "P18": "image of relevant illustration of the subject; if available, use more specific properties (sample: coat of arms image, locator map, flag image, signature image, logo image); only images which exist on Wikimedia Commons are acceptable",
    "P2048": "vertical dimension of an entity",
    "P2519": "Identifier for a person on the website Scope.dk",
    "P1245": "\"Defined Meaning\" on the site OmegaWiki",
    "P345": "identifier from the Internet Movie Database (IMDb) with prefix ('tt', 'nm', 'ch', 'co', or 'ev')",
    "P1248": "identifier for people, places, events, etc. used by some Nordic museums",
    "P349": "identifier for authority control per the National Diet Library of Japan",
    "P2612": "identifier of a topic, in the TED database of talks",
    "P2611": "identifier of a person, in the TED database of talks",
    "P2191": "Identification Number of Fantastique Literature",
    "P2190": "identifier for a person's appearances on C-SPAN",
    "P109": "image of a person's signature",
    "P268": "identifier for the subject issued by BNF (Biblioth\\u00e8que nationale de France). Format: 8 digits followed by a check-digit or letter",
    "P269": "identifier for authority control in the French collaborative library catalog (see also P1025). Format: 8 digits followed by a digit or \"X\"",
    "P906": "identifier per National Library of Sweden Libris library catalog",
    "P902": "identifier in HDS/HLS/DHS/DSS: Historical Dictionary of Switzerland (Q642074), a national encyclopedia",
    "P866": "identifier in Perlentaucher (Q2071388)",
    "P867": "ROME Code for a given occupation in France (V3, 1 letter, 4 digits)",
    "P1839": "identifier assigned by the US Federal Election Commission for federal candidates, parties, and other committees",
    "P865": "identifier per Bayerisches Musiker-Lexikon Online (Q47191)",
    "P863": "identifier in the Indiana Philosophy Ontology project",
    "P3344": "identifies candidates in US elections for national and state government within Project Vote Smart's database",
    "P646": "identifier for a page in the Freebase database. For those starting with \"/g/\", use Google Knowledge Graph identifier (P2671)",
    "P2035": "public autobiographic profile URL of the person at Linkedin.com, don't use on companies",
    "P2031": "start of period during which a person or group flourished (fl. = \"floruit\") in their professional activity",
    "P1036": "use with qualifier \"edition (p747)\" with item value \"DDC 23\" or create new item to represent the corresponding DDC edition",
    "P648": "identifier for works, editions and authors",
    "P1233": "identifier for a person in the Internet Speculative Fiction Database",
    "P3284": "Identifier of person in Yahoo! Japan Talent database",
    "P1728": "identifier for an artist in AllMusic database",
    "P1649": "identifier for a person on the Korean Movie Database (KMDb)",
    "P2538": "Artist identifier for Nationalmuseum in Sweden",
    "P409": "identifier issued by the National Library of Australia (see also P1315 for the newer People Australia identifier)",
    "P2638": "identifier for an entry on TV.com for movies, people and TV series",
    "P2639": "identifier of the German Filmportal.de",
    "P569": "date on which the subject was born",
    "P2924": "identifier for an entry on the official website of the Great Russian Encyclopedia",
    "P3569": "identifier for a concept in the Dutch Cultureel Woordenboek ('Cultural Dictionary')",
    "P3321": "male form of name of an element",
    "P244": "Library of Congress ID for authority control for persons and organizations (for books use P1144)",
    "P245": "identifier from the Getty Union List of Artist Names",
    "P1902": "identifier for an artist on Spotify",
    "P1422": "identifier in the web-based edition of Joachim von Sandrart\\u2019s \"Teutscher Academie der Edlen Bau, Bild- und Mahlerey-K\\u00fcnste\" (1675\\u201380)",
    "P2342": "identifier for a person or institution in the Agorha database (INHA)",
    "P2888": "used to link two concepts, indicating a high degree of confidence that the concepts can be used interchangeably",
    "P2349": "Identifier for a person, in the University of Stuttgart's Database of Scientific Illustrators, 1450-1950",
    "P1695": "identifier for authority control per the National Library of Poland",
    "P2404": "recommended or required concentration limit for chemical exposure in a workplace in a given work day",
    "P2019": "identifier for a person on the AllMovie film database",
    "P2401": "identifier in the Six Degrees of Francis Bacon database",
    "P2013": "identifier for a person or organization in Facebook",
    "P1213": "identifier for a person per National Library of China",
    "P3136": "identifier for a person at the Egypt movie database elCinema",
    "P2950": "identifier in Nomisma.org, a linked open data thesaurus of numismatic concepts",
    "P3348": "Authority ID from the National Library of Greece Authority Records",
    "P2397": "ID of the YouTube channel of a person, or organisation (not to be mixed up with the name of the channel)",
    "P1472": "name of the \"Creator\" page on Wikimedia Commons",
    "P1477": "full name of a person at birth, if different from their current, generally used name (samples: John Peter Doe for Joe Doe, Ann Smith for Ann Miller)",
    "P2390": "title of corresponding article on the Ballotpedia encyclopedia of American politics",
    "P935": "name of the Wikimedia Commons gallery page(s) related to this item (is suitable to allow multiple link to more gallery pages)",
    "P549": "identifier for mathematicians and computer scientists at the Mathematics Genealogy Project",
    "P2561": "name the subject is known by. If a more specific property is available, use that",
    "P1871": "identifier in the Consortium of European Research Libraries thesaurus",
    "P3305": "identifier of a person, in the KINENOTE movie database",
    "P227": "identifier from an international authority file of names, subjects, and organizations (please don't use type n = name, disambiguation)",
    "P949": "identifier for authority control used at the National Library of Israel",
    "P1052": "unknown",
    "P723": "identifier for an author on the DBNL-website for Dutch language authors",
    "P1375": "identifier for an item in the National and University Library in Zagreb (including leading zeroes)",
    "P2174": "identifier assigned to an artist by the Museum of Modern Art in New York",
    "P1670": "identifier for authority control per the Library and Archives Canada",
    "P1273": "identifier for authority control managed by the National Library of Catalonia (BNC)",
    "P1266": "identifier for a person on the AlloCin\\u00e9 film database",
    "P1284": "identifier on the Munzinger Archiv",
    "P1286": "identifier on the Munzinger Archiv",
    "P1280": "identifier\\u00a0in the CONOR.SI database",
    "P1282": "OpenStreetMap tagging schema (a Key:key or Tag:key=value) for classes of things",
    "P1977": "Identifier for an actor/actress/playwright/play, in the lesarchivesduspectacle database of actors/actresses",
    "P1971": "number of children of the person. Mainly in cases where the full list isn't or shouldn't be added in p40.",
    "P918": "NOC/CNP Code for a given occupation in Canada and Qu\\u00e9bec",
    "P919": "Standard Occupational Classification code for US jobs (2010 version)",
    "P1415": "identifier for authority control used by Oxford University Press for online biographical resources, predominantly the Oxford Dictionary of National Biography",
    "P1417": "identifer for an article in the online version of Encyclop\\u00e6dia Britannica",
    "P2471": "identifier of a person in the Models.com website",
    "P1309": "identifier in Bibliotheca Alexandrina",
    "P1899": "author ID for an author represented at LibriVox",
    "P1890": "Biblioteca Nacional de Chile authority file ID",
    "P3477": "identifier of person in Nihon Tarento Meikan",
    "P3222": "ID of article on the Swedish Nationalencyklopedin (NE.se) site",
    "P3479": "identifier for a topic, used by Omni and Aftonbladet",
    "P1256": "Iconclass code that corresponds with an artistic theme or concept. For artworks, use P1257 (depicts Iconclass notation).",
    "P1258": "identifier on Rotten Tomatoes: must use prefix \"m/\" for movies, \"tv/\" for TV series (remove /s number on end unless the item is a specific season), \"celebrity/\" for celebrities, \"critic/\" for film critics, \"source-\" for review sources",
    "P2605": "identifier for a person in the Czech film database \\u010cSFD",
    "P2604": "identifer for a person, in the Kinopoisk.ru database",
    "P2600": "profile on the Geni.com genealogy website",
    "P2919": "media file showing label of this item in sign language. Use \"language of work or name\" (P407) as qualifier to indicate which language",
    "P1157": "identifier for a person on the Biographical Directory of the United States Congress",
    "P1953": "identifier for a band or person in the Discogs database",
    "P1802": "identifier in the Early Modern Letters Online project run by the Bodleian Library",
    "P856": "URL to the website of this item",
    "P1556": "identifier of a person in the Zentralblatt MATH database",
    "P1559": "name of a person in their native language",
    "P3372": "identifier assigned to an artist by the Auckland Art Gallery in New Zealand",
    "P508": "identifier in the subject indexing tool of the National Central Library of Florence",
    "P3192": "identifier for an artist, group or work, on Last.fm",
    "P2750": "identifier used in Photographers' Identities Catalog",
    "P1006": "identifier from the Dutch National Thesaurus for Author names",
    "P2252": "identifier assigned to an artist by the National Gallery of Art in Washington DC",
    "P1005": "identifier for the Portuguese National Library",
    "P1003": "identifier for authority control used at the National Library of Romania",
    "P1711": "identifier in the British Museum person-institution thesaurus",
    "P3106": "identifier for a topic at the Guardian newspaper website",
    "P1938": "author identifier at Project Gutenberg",
    "P2509": "identifier of a film, a person or a cinema in the Movie Walker Database",
    "P373": "name of the Wikimedia Commons category containing files related to this item (without the prefix \"Category:\")",
    "P1138": "unique artist identifier used by Kunstindeks Danmark",
    "P2626": "identifier for a person in the Danish National Filmography",
    "P570": "date on which the subject died",
    "P271": "identifier for an author in CiNii (Scholarly and Academic Information Navigator) \\u00a0(\"DA\" + 8 digits)",
    "P270": "identifier for authority control per CALIS (China Academic Library & Information System)",
    "P3056": "identifier for a person (cast or crew member) in the Turner Classic Movies database",
    "P2188": "Identifier in the BiblioNet database of authors, created by the National Book Centre of Greece, many Greek individual publishers and their professional associations",
    "P3051": "identifier of an article, on http://kindred.stanford.edu/",
    "P1430": "identifier for a person or other subject in the OpenPlaques database - http://openplaques.org/",
    "P2003": "item's username on Instagram",
    "P2002": "this item's username on Twitter",
    "P1749": "entry of described object on Parlement & Politiek, website describing Dutch politics",
    "P1741": "identifier for GTAA, a thesaurus used in audiovisual archives (NISV, EYE)",
    "P1220": "identifier\\u00a0for personnel on Broadway",
    "P2432": "identifier assigned to an artist by the J. Paul Getty Museum",
    "P2435": "PORT-network film database: identifier for a person",
    "P487": "Unicode character representing the item",
    "P1986": "identifier of the Biographical dictionary of italian people",
    "P1982": "identifier for a person on animenewsnetwork.com",
    "P3123": "identifier of a topic in the online Stanford Encyclopedia of Philosophy",
    "P3430": "identifier for items in the Social Networks and Archival Context system",
    "P3435": "identifier for a musician or group in the Video Game Music database",
    "P1442": "picture of a person or animal's grave, gravestone or tomb",
    "P650": "identifier in the RKDartists database (Rijksbureau voor Kunsthistorische Documentatie)",
    "P1563": "identifier of the person's biography in the MacTutor History of Mathematics archive",
    "P1296": "identifier for an item in the Gran Enciclop\\u00e8dia Catalana",
    "P1449": "nickname of an entity",
    "P1051": "identifier in the authority database of the Czech Technical Library",
    "P396": "identifier issued by National Library Service (SBN) of Italy",
    "P3703": "ID of an actor or a company at Japanese Movie Database",
    "P1599": "identifier\\u00a0in the Cambridge Alumni Database/Alumni Cantabrigienses (ACAD)",
    "P3544": "identifier assigned to an artist by the Museum of New Zealand Te Papa Tongarewa",
    "P2332": "identifier for a person in the Dictionary of Art Historians",
    "P1021": "German classification of occupations 2010",
    "P1023": "Dutch classification of occupations SBC maintained by the Dutch CBS (Centraal Bureau voor de Statistiek), 2010 version",
    "P1185": "identifer for a person on rodovid.org",
    "P952": "International Standard Classification of Occupations code",
    "P3221": "identifier for a topic, at the New York Times' website",
    "P950": "identifier from the authority file of the Biblioteca Nacional de Espa\\u00f1a. Format for persons: \"XX\" followed by 4 to 7 digits",
    "P1367": "authority control identifier for artists (creators of publicly owned oil paintings in the UK)",
    "P734": "surname or last name of a person",
    "P735": "first name or another given name of this person. Values used with the property shouldn't link disambiguations nor family names.",
    "P1687": "main Wikidata property for this item",
    "P2418": "identifier for a person in the Structurae database",
    "P1368": "identifier assigned by the National Library of Latvia",
    "P1207": "identifier for authority control in the Center of Warsaw University Library catalog",
    "P3417": "identifier for a topic on Quora (English language version)",
    "P3142": "identifier for a person at the Israeli movie database EDb",
    "P3144": "identifier for a person at the Egypt movie database elFilm",
    "P2949": "identifier for a person in the WikiTree genealogy website",
    "P1969": "identifier for a person on the MovieMeter film database",
    "P2521": "female form of name of an element",
    "P3029": "identifier for a person, family or organisation, in the UK's National Archives database",
    "P1617": "identifier in the BBC Things database",
    "P1614": "identifier on the History of Parliament website",
    "P1963": "when this subject is used as object of \"instance of\" or \"occupation\", the following properties normally apply"
}

wd_object_props = found_obj_props
wd_non_object_props = found_non_object_props
entity_labels = {}


def learn_prop(pr_id):
    if pr_id not in wd_non_object_props and pr_id not in wd_object_props:
        # print 'learning {}...'.format(pr_id)

        wurl = 'http://www.wikidata.org/entity/{}'.format(pr_id)
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
        else:
            wd_non_object_props[pr_id] = description


def get_wd_entity_label(entity_id):
    if entity_id not in entity_labels:
        response = requests.get('http://www.wikidata.org/entity/' + entity_id)
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
    response = requests.get('http://www.wikidata.org/entity/' + entity_id)
    data = response.json()
    d = {'entity': entity_id}

    entity = data['entities'][entity_id]
    claims = entity['claims']
    if entity['descriptions']:
        description = entity.get('descriptions', {}).get('en', {}).get('value', '')
        d['description'] = description
    d['aliases'] = list(get_field_values(entity['aliases']))
    d['labels'] = list(get_field_values(entity['labels']))
    for pr_id in filter(lambda x: x in pr_filter, claims):
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
mapped_properties = set.union(set(found_non_object_props.keys()), set(found_obj_props.keys()))


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
    if pid in found_obj_props:
        obj = True
        desc = found_obj_props.get(pid)
    elif pid in found_non_object_props:
        obj = False
        desc = found_non_object_props.get(pid)
    else:
        response = jsonify({'message': 'not found'})
        response.status_code = 404
        return response

    return jsonify({'object': obj, 'description': desc})


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5015, use_reloader=False, debug=False, threaded=True)

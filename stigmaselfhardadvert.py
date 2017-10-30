from kafka import KafkaConsumer
from kafka import KafkaClient
from kafka import SimpleProducer
import logging
import sys
import json

topic_name = sys.argv[1]

server = 'gatezkt1.storm:9092'
kafka = KafkaClient(server)
out = SimpleProducer(kafka)
raw_in = KafkaConsumer(topic_name, group_id='head', bootstrap_servers=[server], auto_offset_reset='smallest')

for message in raw_in:
#       parsed = json.loads(message)
        parsed = json.loads(message.value.decode("utf-8"))
        text = parsed['text']
        if ('stigma' in text.lower() or '#mentalillness' in text.lower() or '#mentalilness' in text.lower()):
                classificationstig  = 1
        else:
                classificationstig  = 0
        if "http" in text.lower() and ("discount" in text.lower() or "buy" in text.lower() or "online" in text.lower() or "best price" in text.lower() or "next day delivery" in text.lower() or "cheap" in text.lower() or "compare prices" in text.lower() or "save money" in text.lower() or "no prescription" in text.lower() or "no rx" in text.lower() or "without prescription" in text.lower() or "without rx" in text.lower() or "overnight delivery" in text.lower() or "viagra" in text.lower() or "adpharm" in text.lower() or "for sale" in text.lower() or "shipping" in text.lower() or "lowest price" in text.lower() or "purchase" in text.lower() or "free delivery" in text.lower() or "uk pharmacy" in text.lower() or "free delivery" in text.lower()):
                classificationad = 1
        else:
                classificationad = 0
        if 'suicid' in text.lower():
                if  'suicide girl' in text.lower() or 'political suicide' in text.lower() or 'corporate suicide' in text.lower() or 'social suicide' in text.lower() or 'twitter suicide' in text.lower() or 'political suicide' in text.lower() or 'suicidal tendencies -' in text.lower() or 'suicidal tendencies-' in text.lower() or ('suicidal tendencies'in text.lower() and 'band ' in text.lower()) or 'song' in text.lower() or 'suicide diar' in text.lower() or 'suicidebl' in text.lower() or 'listening to' in text.lower() or 'suicidegi' in text.lower() or 'cthagod' in text.lower() or 'career suicide' in text.lower() or 'bunny suicide' in text.lower() or 'suicide silence' in text.lower() or 'suicide sunday' in text.lower() or 'commercial suicide' in text.lower() or 'bird' in text.lower() or ('work out' in text.lower() and 'suicide' in text.lower()) or ('working out' in text.lower() and 'suicide' in text.lower()) or 'self righteous suicide' in text.lower() or 'immitation is suicide' in text.lower() or 'suicidal tenden' in text.lower() or '@suic' in text.lower() or 'band' in text.lower() or 'music' in text.lower() or 'running suic' in text.lower() or 'spiritual suic' in text.lower():
                        suicideclass = 0
                else:
                        suicideclass = 1
        else:
                suicideclass = 0
        parsed['features'] = {}
        parsed['features']['Anti-Stigma'] = classificationstig
        parsed['features']['Advert'] = classificationad
        parsed['features']['Suiciderelevant'] = suicideclass
        try:
                out.send_messages('med_advert',json.dumps(parsed).encode("utf-8"))
                print "success"
        except:
                print "fail"
                pass

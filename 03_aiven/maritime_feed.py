"""
Listen to AIS stream from aisstream.io and publish vessel data to kafka.
"""

import asyncio
import json
import logging
import os
import sys

import confluent_kafka
import websockets

from util.kafka_util import get_kafka_ssl_config

logger = logging.getLogger(__name__)

AIS_WS_URL = "wss://stream.aisstream.io/v0/stream"
API_KEY = os.environ["AIS_API_KEY"]


def set_logger():
    logging.basicConfig(
        stream=sys.stdout,
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )


# AIS vessel type code classification
# See: https://coast.noaa.gov/data/marinecadastre/ais/VesselTypeCodes2018.pdf
def classify_vessel(ais_type: int, mmsi: int) -> str:
    """Classify a vessel by its AIS type code into a rendering category."""
    if 80 <= ais_type <= 89:
        return "tanker"
    if 70 <= ais_type <= 79:
        return "cargo"
    if 60 <= ais_type <= 69:
        return "passenger"
    if ais_type in (36, 37):
        return "yacht"
    if ais_type == 35:
        return "military_vessel"
    mmsi_str = str(mmsi)
    if mmsi_str.startswith("3380") or mmsi_str.startswith("3381"):
        return "military_vessel"
    if ais_type in (30, 31, 32, 33, 34):
        return "other"
    if ais_type in (50, 51, 52, 53, 54, 55, 56, 57, 58, 59):
        return "other"
    return "unknown"


# MMSI Maritime Identification Digit (MID) → Country mapping
MID_COUNTRY = {
    201: "Albania",
    202: "Andorra",
    203: "Austria",
    204: "Portugal",
    205: "Belgium",
    206: "Belarus",
    207: "Bulgaria",
    208: "Vatican",
    209: "Cyprus",
    210: "Cyprus",
    211: "Germany",
    212: "Cyprus",
    213: "Georgia",
    214: "Moldova",
    215: "Malta",
    216: "Armenia",
    218: "Germany",
    219: "Denmark",
    220: "Denmark",
    224: "Spain",
    225: "Spain",
    226: "France",
    227: "France",
    228: "France",
    229: "Malta",
    230: "Finland",
    231: "Faroe Islands",
    232: "United Kingdom",
    233: "United Kingdom",
    234: "United Kingdom",
    235: "United Kingdom",
    236: "Gibraltar",
    237: "Greece",
    238: "Croatia",
    239: "Greece",
    240: "Greece",
    241: "Greece",
    242: "Morocco",
    243: "Hungary",
    244: "Netherlands",
    245: "Netherlands",
    246: "Netherlands",
    247: "Italy",
    248: "Malta",
    249: "Malta",
    250: "Ireland",
    251: "Iceland",
    252: "Liechtenstein",
    253: "Luxembourg",
    254: "Monaco",
    255: "Portugal",
    256: "Malta",
    257: "Norway",
    258: "Norway",
    259: "Norway",
    261: "Poland",
    263: "Portugal",
    264: "Romania",
    265: "Sweden",
    266: "Sweden",
    267: "Slovakia",
    268: "San Marino",
    269: "Switzerland",
    270: "Czech Republic",
    271: "Turkey",
    272: "Ukraine",
    273: "Russia",
    274: "North Macedonia",
    275: "Latvia",
    276: "Estonia",
    277: "Lithuania",
    278: "Slovenia",
    301: "Anguilla",
    303: "Alaska",
    304: "Antigua",
    305: "Antigua",
    306: "Netherlands Antilles",
    307: "Aruba",
    308: "Bahamas",
    309: "Bahamas",
    310: "Bermuda",
    311: "Bahamas",
    312: "Belize",
    314: "Barbados",
    316: "Canada",
    319: "Cayman Islands",
    321: "Costa Rica",
    323: "Cuba",
    325: "Dominica",
    327: "Dominican Republic",
    329: "Guadeloupe",
    330: "Grenada",
    331: "Greenland",
    332: "Guatemala",
    334: "Honduras",
    336: "Haiti",
    338: "United States",
    339: "Jamaica",
    341: "Saint Kitts",
    343: "Saint Lucia",
    345: "Mexico",
    347: "Martinique",
    348: "Montserrat",
    350: "Nicaragua",
    351: "Panama",
    352: "Panama",
    353: "Panama",
    354: "Panama",
    355: "Panama",
    356: "Panama",
    357: "Panama",
    358: "Puerto Rico",
    359: "El Salvador",
    361: "Saint Pierre",
    362: "Trinidad",
    364: "Turks and Caicos",
    366: "United States",
    367: "United States",
    368: "United States",
    369: "United States",
    370: "Panama",
    371: "Panama",
    372: "Panama",
    373: "Panama",
    374: "Panama",
    375: "Saint Vincent",
    376: "Saint Vincent",
    377: "Saint Vincent",
    378: "British Virgin Islands",
    379: "US Virgin Islands",
    401: "Afghanistan",
    403: "Saudi Arabia",
    405: "Bangladesh",
    408: "Bahrain",
    410: "Bhutan",
    412: "China",
    413: "China",
    414: "China",
    416: "Taiwan",
    417: "Sri Lanka",
    419: "India",
    422: "Iran",
    423: "Azerbaijan",
    425: "Iraq",
    428: "Israel",
    431: "Japan",
    432: "Japan",
    434: "Turkmenistan",
    436: "Kazakhstan",
    437: "Uzbekistan",
    438: "Jordan",
    440: "South Korea",
    441: "South Korea",
    443: "Palestine",
    445: "North Korea",
    447: "Kuwait",
    450: "Lebanon",
    451: "Kyrgyzstan",
    453: "Macao",
    455: "Maldives",
    457: "Mongolia",
    459: "Nepal",
    461: "Oman",
    463: "Pakistan",
    466: "Qatar",
    468: "Syria",
    470: "UAE",
    472: "Tajikistan",
    473: "Yemen",
    475: "Tonga",
    477: "Hong Kong",
    478: "Bosnia",
    501: "Antarctica",
    503: "Australia",
    506: "Myanmar",
    508: "Brunei",
    510: "Micronesia",
    511: "Palau",
    512: "New Zealand",
    514: "Cambodia",
    515: "Cambodia",
    516: "Christmas Island",
    518: "Cook Islands",
    520: "Fiji",
    523: "Cocos Islands",
    525: "Indonesia",
    529: "Kiribati",
    531: "Laos",
    533: "Malaysia",
    536: "Northern Mariana Islands",
    538: "Marshall Islands",
    540: "New Caledonia",
    542: "Niue",
    544: "Nauru",
    546: "French Polynesia",
    548: "Philippines",
    553: "Papua New Guinea",
    555: "Pitcairn",
    557: "Solomon Islands",
    559: "American Samoa",
    561: "Samoa",
    563: "Singapore",
    564: "Singapore",
    565: "Singapore",
    566: "Singapore",
    567: "Thailand",
    570: "Tonga",
    572: "Tuvalu",
    574: "Vietnam",
    576: "Vanuatu",
    577: "Vanuatu",
    578: "Wallis and Futuna",
    601: "South Africa",
    603: "Angola",
    605: "Algeria",
    607: "Benin",
    609: "Botswana",
    610: "Burundi",
    611: "Cameroon",
    612: "Cape Verde",
    613: "Central African Republic",
    615: "Congo",
    616: "Comoros",
    617: "DR Congo",
    618: "Ivory Coast",
    619: "Djibouti",
    620: "Egypt",
    621: "Equatorial Guinea",
    622: "Ethiopia",
    624: "Eritrea",
    625: "Gabon",
    626: "Gambia",
    627: "Ghana",
    629: "Guinea",
    630: "Guinea-Bissau",
    631: "Kenya",
    632: "Lesotho",
    633: "Liberia",
    634: "Liberia",
    635: "Liberia",
    636: "Liberia",
    637: "Libya",
    642: "Madagascar",
    644: "Malawi",
    645: "Mali",
    647: "Mauritania",
    649: "Mauritius",
    650: "Mozambique",
    654: "Namibia",
    655: "Niger",
    656: "Nigeria",
    657: "Guinea",
    659: "Rwanda",
    660: "Senegal",
    661: "Sierra Leone",
    662: "Somalia",
    663: "South Africa",
    664: "Sudan",
    667: "Tanzania",
    668: "Togo",
    669: "Tunisia",
    670: "Uganda",
    671: "Egypt",
    672: "Tanzania",
    674: "Zambia",
    675: "Zimbabwe",
    676: "Comoros",
    677: "Tanzania",
}


def get_country_from_mmsi(mmsi: int) -> str:
    """Look up flag state from MMSI Maritime Identification Digit."""
    mmsi_str = str(mmsi)
    if len(mmsi_str) == 9:
        mid = int(mmsi_str[:3])
        return MID_COUNTRY.get(mid, "UNKNOWN")
    return "UNKNOWN"


def on_delivery(err: confluent_kafka.KafkaError, msg: confluent_kafka.Message):
    if err is not None:
        logger.error(f"Delivery failed for {msg.topic()}[{msg.key()}]: {err}")
    else:
        logger.debug(f"Delivered to {msg.topic()}[{msg.partition()}] @ {msg.offset()}")


def on_error(err: confluent_kafka.KafkaError):
    logger.error(f"Producer error: {err}")


async def run_ais_stream(producer: confluent_kafka.Producer) -> None:
    """Connect to aisstream.io websocket and publish AIS messages to kafka."""
    async with websockets.connect(AIS_WS_URL) as ws:
        await ws.send(
            json.dumps(
                {
                    "APIKey": API_KEY,
                    "BoundingBoxes": [[[-90, -180], [90, 180]]],
                    "FilterMessageTypes": [
                        "PositionReport",
                        "ShipStaticData",
                        "StandardClassBPositionReport",
                    ],
                }
            )
        )
        logger.info("AIS Stream connected — receiving vessel data")

        msg_count = 0
        async for raw_msg in ws:
            try:
                data = json.loads(raw_msg)
            except json.JSONDecodeError:
                continue

            if "error" in data:
                logger.error(f"AIS Stream error: {data['error']}")
                continue

            msg_type = data.get("MessageType", "")
            metadata = data.get("MetaData", {})
            message = data.get("Message", {})

            mmsi = metadata.get("MMSI", 0)
            if not mmsi:
                continue

            value = None

            if msg_type in ("PositionReport", "StandardClassBPositionReport"):
                report = message.get(msg_type, {})
                lat = report.get("Latitude", metadata.get("latitude", 0))
                lng = report.get("Longitude", metadata.get("longitude", 0))

                if lat == 0 and lng == 0:
                    continue
                if abs(lat) > 90 or abs(lng) > 180:
                    continue

                heading = report.get("TrueHeading", 511)
                value = {
                    "msg_type": "position",
                    "mmsi": mmsi,
                    "name": (metadata.get("ShipName", "UNKNOWN").strip() or "UNKNOWN"),
                    "lat": round(lat, 5),
                    "lng": round(lng, 5),
                    "sog": round(report.get("Sog", 0), 1),
                    "cog": round(report.get("Cog", 0), 1),
                    "heading": heading if heading != 511 else report.get("Cog", 0),
                    "country": get_country_from_mmsi(mmsi),
                }

            elif msg_type == "ShipStaticData":
                static = message.get("ShipStaticData", {})
                ais_type = static.get("Type", 0)
                value = {
                    "msg_type": "static",
                    "mmsi": mmsi,
                    "name": (
                        static.get("Name", "") or metadata.get("ShipName", "UNKNOWN")
                    ).strip()
                    or "UNKNOWN",
                    "callsign": (static.get("CallSign", "") or "").strip(),
                    "imo": static.get("ImoNumber", 0),
                    "destination": (static.get("Destination", "") or "")
                    .strip()
                    .replace("@", ""),
                    "type": classify_vessel(ais_type, mmsi),
                    "country": get_country_from_mmsi(mmsi),
                }

            if value is None:
                continue
            else:
                print(json.dumps(value).encode())

            producer.produce(
                topic="ais",
                value=json.dumps(value).encode(),
                key=str(mmsi),
                on_delivery=on_delivery,
            )
            producer.poll(0.0)

            msg_count += 1
            if msg_count % 5000 == 0:
                logger.info(f"AIS Stream: published {msg_count} messages")


def main():
    set_logger()
    producer = confluent_kafka.Producer(
        {
            "logger": logger,
            **get_kafka_ssl_config(),
        },
        error_cb=on_error,
    )
    while True:
        try:
            asyncio.run(run_ais_stream(producer))
        except KeyboardInterrupt:
            logger.info("Stopped by user")
            break
        except websockets.WebSocketException:
            logger.exception("Websocket error, restarting")


if __name__ == "__main__":
    main()

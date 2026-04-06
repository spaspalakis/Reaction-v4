from kafka import KafkaProducer
import json

# Kafka settings
KAFKA_SERVER_WIREGUARD = "10.147.0.1:9094"
# KAFKA_SERVER_PUBLIC = "apps.edutel.uniwa.gr:9092"
TOPIC_NAME = "PathPlanning_Input"

# Create Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER_WIREGUARD,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks=1  # Wait for Kafka to confirm receipt
)

# Sample message
our_msg = {
  "PathPlanning_input": {
    "type": "FeatureCollection",
    "features": [
      {
        "type": "Feature",
        "properties": {
          "altitude": 30,
          "percentageOfSidelap": 50,
          "speed": 5,
          "frontlap": 20.3,
          "numberOfDrones": 2,
          "pathsStrictlyInPoly": True,
          "droneInfo": [
            {
              "ID": 0,
              "portions": 0.5
            }
          ]
        },
        "geometry": {
          "type": "Polygon",
          "coordinates": [
            [
              ["lon1", "lat1"],
              ["lon2", "lat2"],
              ["lon3", "lat3"],
              ["lon1", "lat1"]
            ]
          ]
        },
        "initialPositions": {
          "type": "MultiPoint",
          "coordinates": [
            ["lon1", "lat1"],
            ["lon2", "lat2"]
          ]
        }
      }
    ]
  }
}
# Sample message
ekpa_msg = {
  "MissionName": "Test Mission 3",
  "MissionId": "35c2ad16-6663-47e0-9cd3-9f52117bb5a6",
  "Description": "Polygon-only mission",
  "MissionType": "Testing",
  "Issuer": {
    "IssuerName": "Kostas",
    "Certificate": "string",
    "PublicKey": "string",
    "UUID": "string",
    "Param1": "",
    "Param2": ""
  },
  "properties": {
    "number_of_drones": 2,
    "paths_strictly_in_poly": "True",
    "percentage_of_sidelap": 72,
    "scanning_density": 90,
    "speed": 22,
    "altitude": 100,
    "frontlap": 20,
    "portions": [
      0.2,
      0.8
    ]
  },
  "Route": [],
  "Geometries": [
    {
      "type": "Feature",
      "properties": {
        "GeometryID": "1f8eff46-d41c-4604-8814-e55c689f2d53",
        "GeometryName": "Polygon 1",
        "GeometryAction": "cover",
        "Elevation": "0"
      },
      "geometry": {
        "type": "Polygon",
        "coordinates": [
          [
            [
              23.74843805859978,
              37.97638404818811
            ],
            [
              23.79031809636623,
              37.97381302651111
            ],
            [
              23.7678333219916,
              37.96339270089696
            ],
            [
              23.74843805859978,
              37.97638404818811
            ]
          ]
        ]
      },
      "initialPositions": {
        "type": "MultiPoint",
        "coordinates": [
          [
            23.7425295207815,
            38.112327069292
          ],
          [
            23.7425295207815,
            38.112327069292
          ]
        ]
      }
    }
  ]
}
ekpa_msg_NFZs = {
  "MissionName": "Dummy Mission",
  "MissionId": "bfb49105-bcce-4cee-ac20-a21708163e14",
  "Description": "Polygon-only mission",
  "MissionType": "Testing",
  "Issuer": {
    "IssuerName": "Kostas_UOA",
    "Certificate": "string",
    "PublicKey": "string",
    "UUID": "string",
    "Param1": "",
    "Param2": ""
  },
  "properties": {
    "number_of_drones": 1,
    "paths_strictly_in_poly": "True",
    "percentage_of_sidelap": 72,
    "scanning_density": 150,
    "speed": 15,
    "altitude": 100,
    "frontlap": 20,
    "portions": [
      1
    ]
  },
  "Route": [],
  "Geometries": [
    {
      "type": "Feature",
      "properties": {
        "GeometryID": "30a5d63c-a94b-44bc-abdd-bb7438ea8118",
        "GeometryName": "Polygon 2",
        "GeometryAction": "NoFlyZones",
        "Elevation": "0"
      },
      "geometry": {
        "type": "Polygon",
        "coordinates": [
          [
            [
              23.76554480688439,
              37.97632907662732
            ],
            [
              23.77198128809848,
              37.97565250019989
            ],
            [
              23.77103727085374,
              37.97138992529919
            ],
            [
              23.76640300437962,
              37.9723371855569
            ],
            [
              23.76554480688439,
              37.97632907662732
            ]
          ]
        ]
      },
      "initialPositions": {
        "type": "MultiPoint",
        "coordinates": [
          [
            23.767502,
            37.968426
          ]
        ]
      }
    },
    {
      "type": "Feature",
      "properties": {
        "GeometryID": "7b1da0e4-2f1e-41bc-abe3-c314911e870a",
        "GeometryName": "Polygon 1",
        "GeometryAction": "RegionOfInterest",
        "Elevation": "0"
      },
      "geometry": {
        "type": "Polygon",
        "coordinates": [
          [
            [
              23.75335840245233,
              37.96895405708128
            ],
            [
              23.78150728029537,
              37.96421741529657
            ],
            [
              23.78459679127814,
              37.97917062951849
            ],
            [
              23.76554480688439,
              37.9817414635264
            ],
            [
              23.76460078963965,
              37.9778175228148
            ],
            [
              23.75619045418654,
              37.97923828419884
            ],
            [
              23.75335840245233,
              37.96895405708128
            ]
          ]
        ]
      },
      "initialPositions": {
        "type": "MultiPoint",
        "coordinates": [
          [
            23.767502,
            37.968426
          ]
        ]
      }
    }
  ]
}
# Send message
future = producer.send(TOPIC_NAME, key=b"force_partition", value=ekpa_msg_NFZs, partition=0)
result = future.get(timeout=5)  # This will raise an error if Kafka rejects the message
print("Message sent successfully!")

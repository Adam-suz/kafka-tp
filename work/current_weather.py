from kafka import KafkaProducer
import requests
import json
import time

# Configuration Kafka
KAFKA_SERVER = "localhost:9092"
TOPIC = "weather_stream"

# Création du producteur Kafka
producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Liste des villes
cities = [
    {
        "city": "Paris",
        "country": "France",
        "latitude": 48.8566,
        "longitude": 2.3522
    },
    {
        "city": "Seville",
        "country": "Spain",
        "latitude": 37.3891,
        "longitude": -5.9845
    },
    {
        "city": "Dubai",
        "country": "United Arab Emirates",
        "latitude": 25.2048,
        "longitude": 55.2708
    },
    {
        "city": "Reykjavik",
        "country": "Iceland",
        "latitude": 64.1466,
        "longitude": -21.9426
    },
    {
        "city": "Wellington",
        "country": "New Zealand",
        "latitude": -41.2865,
        "longitude": 174.7762
    }
]

# URL de l'API Open-Meteo
API_URL = "https://api.open-meteo.com/v1/forecast"

print("Producteur météo démarré...")
print("Envoi des données toutes les 30 secondes...")

try:
    while True:

        for city in cities:

            params = {
                "latitude": city["latitude"],
                "longitude": city["longitude"],
                "current": "temperature_2m,wind_speed_10m,weather_code",
                "wind_speed_unit": "ms"
            }

            try:
                # Récupération des données météo
                response = requests.get(
                    API_URL,
                    params=params,
                    timeout=10
                )

                response.raise_for_status()

                weather_data = response.json()

                # Ajout de la ville et du pays
                weather_data["city"] = city["city"]
                weather_data["country"] = city["country"]

                # Envoi dans Kafka
                producer.send(TOPIC, value=weather_data).get(timeout=10)

                print(
                    f"Message envoyé : {city['city']} "
                    f"| Température : {weather_data['current']['temperature_2m']} °C "
                    f"| Vent : {weather_data['current']['wind_speed_10m']} m/s"
                )

            except (requests.RequestException, ValueError, KeyError) as e:
                print(f"Erreur API pour {city['city']} : {e}")

            except Exception as e:
                print(f"Erreur Kafka pour {city['city']} : {e}")

        # Attendre 30 secondes avant le prochain cycle
        time.sleep(30)

except KeyboardInterrupt:
    print("\nArrêt du producteur météo.")

finally:
    producer.flush()
    producer.close()

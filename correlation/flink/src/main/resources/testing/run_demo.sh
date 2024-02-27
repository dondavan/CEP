tail -F /Users/taaxuha1/Desktop/World-Huaweii-Pro/Swisscom/correlation-cep/correlation/flink/src/main/resources/testing/log/S2S.log | grep SitetoSiteFailureTrigger

kcat -b localhost:9092 -t zabbix_events

kcat -t aggregation-alerts -b localhost:9092 -u 2>&1 | grep {
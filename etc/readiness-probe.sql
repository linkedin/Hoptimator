
!connect "jdbc:calcite:model=/etc/config/model.yaml" "" ""

SELECT * FROM DATAGEN.PERSON;
SELECT * FROM DATAGEN.COMPANY;
SELECT * FROM RAWKAFKA."test-sink" LIMIT 0;

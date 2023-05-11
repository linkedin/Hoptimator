
!set maxWidth 80
!table
!schemas

-- built-in bounded tables
SELECT * FROM DATAGEN.PERSON;
SELECT * FROM DATAGEN.COMPANY;

-- MySQL CDC tables
SELECT * FROM INVENTORY."products_on_hand" LIMIT 1;

-- MySQL CDC -> Kafka
SELECT * FROM RAWKAFKA."products" LIMIT 1;


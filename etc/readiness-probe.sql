
!connect "jdbc:calcite:model=./model.yaml" "" ""

!set maxWidth 80
!table
!schemas

SELECT * FROM DATAGEN.PERSON;
SELECT * FROM DATAGEN.COMPANY;


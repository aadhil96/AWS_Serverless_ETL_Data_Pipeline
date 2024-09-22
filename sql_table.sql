 CREATE TABLE IF NOT EXISTS titanic_data (
            PassengerId INT PRIMARY KEY,
            Survived INT,
            Pclass INT,
            Name VARCHAR(255),
            Sex VARCHAR(10),
            Age FLOAT,
            SibSp INT,
            Parch INT,
            Ticket VARCHAR(50),
            Fare FLOAT,
            Cabin VARCHAR(50),
            Embarked VARCHAR(5)
        );
USE covid19;
CREATE TABLE daily_covid19_reports (
  id INT(6) UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  txn_date DATETIME,
  new_case INT(10),
  total_case INT(10),
  total_case_excludeabroad INT(10),
  new_death INT(10),
  total_death INT(10),
  new_recovered INT(10),
  total_recovered" INT(10),
  update_date DATETIME
);

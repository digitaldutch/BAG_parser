from database_sqlite import DatabaseSqlite

sql = """
      SELECT
          a.huisnummer,
          a.huisletter || a.toevoeging AS toevoeging,
          a.postcode,
          w.naam as woonplaats
      FROM adressen a
      LEFT JOIN woonplaatsen w ON a.woonplaats_id = w.id
          LEFT JOIN openbare_ruimten o ON a.openbare_ruimte_id = o.id
          
      """


if __name__ == '__main__':
    database = DatabaseSqlite()

    database.cursor.execute(sql)

    count = 0
    for row in database.cursor:
        if count % 1000 == 0:
            print(count)
        count += 1

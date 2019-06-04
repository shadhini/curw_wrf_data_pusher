import traceback
import pymysql


def update_run_table():
    # Connect to the database
    connection = pymysql.connect(host='35.230.102.148',
            user='root',
            password='cfcwm07',
            db='curw_fcst',
            cursorclass=pymysql.cursors.DictCursor)

    results = {}

    try:
        # Extract station ids
        with connection.cursor() as cursor1:
            sql_statement = "select id, latitude, longitude from station;"
            cursor1.execute(sql_statement)
            results = cursor1.fetchall()

        with connection.cursor() as cursor3:
            for result in results:
                sql_statement = "UPDATE `curw_fcst`.`station` SET `name`=%s WHERE `id`=%s;"
                cursor3.execute(sql_statement, ('wrf_{}_{}'.format(result.get('latitude'), result.get('longitude')),
                                                result.get('id')))

        connection.commit()

    except Exception as ex:
        traceback.print_exc()
    finally:
        connection.close()


update_run_table()

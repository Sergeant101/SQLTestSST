using Npgsql;
using static System.Console;

#nullable disable

namespace SQLTest.DataAccessLayer
{

    // Общее примечание: чисто теоретически в JOINах при объединении в ON 
    // указываются все объединяемые параметры для надёжности
    // я этот момент опустил для уменьшения кода и ускорения написания тестового
    internal static class SQLTasks
    {
        internal static string connectionString = "Host = 192.168.0.156; Port = 5432; Database = TestSST; Username = remoteuser; Password = qwerty";

        internal static void SQLTask_Tst(int aggregate)
        {
            NpgsqlConnection con = null;
            try
            {
                con = new NpgsqlConnection(connectionString);

                var scmd = "SELECT * FROM location_dict";

                using var cmd = new NpgsqlCommand(scmd, con);

                con.Open();

                NpgsqlDataReader nsdr = cmd.ExecuteReader();

                while(nsdr.Read())
                {
                    WriteLine(nsdr["id"].ToString() + nsdr["name"].ToString());
                }

            }
            catch(Exception e)
            {
                WriteLine("OOPs, something went wrong. " + e);
            }
            finally
            {
                con.Close();
            }
        }

        //Задание 1 - Вывести список агрегатов, у которых количество параметров больше, 
        //чем у группового агрегата.
        internal static void SQLTask_1(int aggregate)
        {
            NpgsqlConnection con = null;
            try
            {
                con = new NpgsqlConnection(connectionString);

                //Если уж делаю запросы из программы продемонстрирую свои навыки параметризованных запросов
                var scmd =  @"SELECT * 
                            FROM unit_dict 
                            WHERE parameter_count > 
                                (SELECT SUM (parameter_count) 
                                FROM unit_dict 
                                WHERE parent_id = @aggregate)";

                using var cmd = new NpgsqlCommand(scmd, con);

                cmd.Parameters.AddWithValue("@aggregate", aggregate);

                con.Open();

                NpgsqlDataReader nsdr = cmd.ExecuteReader();

                while(nsdr.Read())
                {
                    WriteLine(nsdr["unit_id"].ToString() + " " + nsdr["name"].ToString());
                }

            }
            catch(Exception e)
            {
                WriteLine("OOPs, something went wrong. " + e);
            }
            finally
            {
                con.Close();
            }
        }

        //Задание 2
        //Вывести список агрегатов, содержащих максимальное количество параметров 
        //в своем участке, отсортированный по убыванию количества параметров.
        internal static void SQLTask_2()
        {
            NpgsqlConnection con = null;
            try
            {
                con = new NpgsqlConnection(connectionString);

                var scmd =  @"WITH temp_selection AS (
                            SELECT parent_id, MAX(parameter_count) maxpc 
                            FROM unit_dict 
                            WHERE parent_id IS NOT NULL 
                            GROUP BY parent_id) 
                            SELECT * 
                            FROM temp_selection AS ts 
                            JOIN unit_dict AS ud 
                            ON ts.parent_id = ud.parent_id 
                            AND ts.maxpc = ud.parameter_count 
                            ORDER BY ts.maxpc DESC";

                using var cmd = new NpgsqlCommand(scmd, con);

                con.Open();

                NpgsqlDataReader nsdr = cmd.ExecuteReader();

                while(nsdr.Read())
                {
                    WriteLine(nsdr["parent_id"].ToString() + " " + nsdr["maxpc"].ToString() + " " + nsdr["name"].ToString());
                }

            }
            catch(Exception e)
            {
                Console.WriteLine("OOPs, something went wrong. " + e);
            }
            finally
            {
                con.Close();
            }
        }

        // Задание 3
        // Вывести список ID участков, количество агрегатов которых меньше 3-х штук.
        internal static void SQLTask_3()
        {
            NpgsqlConnection con = null;
            try
            {
                con = new NpgsqlConnection(connectionString);

                var scmd =  @"WITH temp_selection AS (
                            SELECT parent_id, count(parent_id) cpi 
                            FROM unit_dict 
                            WHERE parent_id IS NOT NULL 
                            GROUP BY parent_id) 
                            SELECT * 
                            FROM temp_selection 
                            WHERE cpi < 3 AND cpi IS NOT NULL";

                using var cmd = new NpgsqlCommand(scmd, con);

                con.Open();

                NpgsqlDataReader nsdr = cmd.ExecuteReader();

                while(nsdr.Read())
                {
                    WriteLine(nsdr["parent_id"].ToString() + " " + nsdr["cpi"].ToString());
                }

            }
            catch(Exception e)
            {
                Console.WriteLine("OOPs, something went wrong. " + e);
            }
            finally
            {
                con.Close();
            }
        }

        //Задание 4
        //Вывести список ID групповых агрегатов, 
        //количество параметров которых больше 
        //или равно суммарному количеству параметров его дочерних агрегатов.
        //====================================================================
        // Я не до конца понял задание, возможно вместо ID групповых агрегатов 
        // дожно стоять количество параметров участков
        // т.к. у групповых агрегатов в представленной таблице количество параметров всегда будет
        // равно суммарном количеству параметров его дочерних агрегатов

        // Задание 5
        // Найти список наименований участков с минимальным количеством параметров.
        internal static void SQLTask_5()
        {
            NpgsqlConnection con = null;
            try
            {
                con = new NpgsqlConnection(connectionString);

                var scmd =  @"CREATE TEMP TABLE temp_table AS 
                            SELECT location_id, SUM(parameter_count) spc 
                            FROM unit_dict 
                            GROUP BY location_id; 
                            SELECT * 
                            FROM temp_table AS tt 
                            JOIN location_dict AS ind
                            ON tt.location_id = ind.id 
                            WHERE spc = (
                                SELECT MIN(spc) 
                                FROM temp_table) " ;                
  
                using var cmd = new NpgsqlCommand(scmd, con);

                con.Open();

                NpgsqlDataReader nsdr = cmd.ExecuteReader();

                while(nsdr.Read())
                {
                    WriteLine(nsdr["name"].ToString() + " - параметров " + nsdr["spc"].ToString());
                }

            }
            catch(Exception e)
            {
                Console.WriteLine("OOPs, something went wrong. " + e);
            }
            finally
            {
                con.Close();
            }
        }

        // Задание 6
        // Вывести номера труб, прошедшие обработку с 01.05.2016 по 20.05.2016 
        // и продолжительность последней (в этом периоде) обработки. 
        internal static void SQLTask_6()
        {
            NpgsqlConnection con = null;
            try
            {
                con = new NpgsqlConnection(connectionString);

                var scmd =  @"SELECT pass_id, duration 
                            FROM unit_passes 
                            WHERE dt >= '2016-05-01'
                            AND dt <= '2016-05-20'";               
  
                using var cmd = new NpgsqlCommand(scmd, con);

                con.Open();

                NpgsqlDataReader nsdr = cmd.ExecuteReader();

                while(nsdr.Read())
                {
                    WriteLine(nsdr["pass_id"].ToString() + " " + nsdr["duration"].ToString() );
                }

            }
            catch(Exception e)
            {
                Console.WriteLine("OOPs, something went wrong. " + e);
            }
            finally
            {
                con.Close();
            }
        }

        // Задание 7
        // Вывести номера труб, которые не прошли агрегат 22. Без использования подзапросов, только соединение таблиц. 
        //===========================================================================================================
        // Я не мог выполнить этого задания - я не настолько продвинутый в бэкэнде.

        // Задание 8
        // Найти проходы с продукцией, наименование которых начинается на "итз" 
        //(в независимости от регистра) и вывести их в порядке возрастания даты обработки. 
        //Нужно вывести: Номер трубы, дату обработки, продолжительность обработки, 
        //продолжительность предыдущей (по дате) обработки этой же продукции.
            internal static void SQLTask_8()
        {
            NpgsqlConnection con = null;
            try
            {
                con = new NpgsqlConnection(connectionString);

                var scmd =  @"  CREATE TEMP TABLE temp_table AS 
                                SELECT  up.pass_id AS tpass_id, 
                                        up.dt AS tdt, 
                                        up.duration AS tduration,                                         
                                        pi.pipe_no AS tpipe, 
                                        up.parent_pass_id AS tparent_pass_id 
                                FROM unit_passes AS up 
                                JOIN pipes AS pi 
                                ON pi.matid = up.matid 
                                WHERE pi.pipe_no ILIKE 'итз%'; 
                                SELECT  tpass_id, 
                                        tdt, 
                                        tduration, 
                                        tpipe, 
                                        up.dt AS previous_data 
                                FROM temp_table AS tt 
                                JOIN unit_passes AS up 
                                ON tt.tparent_pass_id = up.pass_id
                                ORDER BY tdt ";               
  
                using var cmd = new NpgsqlCommand(scmd, con);

                con.Open();

                NpgsqlDataReader nsdr = cmd.ExecuteReader();

                while(nsdr.Read())
                {
                    WriteLine(nsdr["tpass_id"].ToString() + "    " + nsdr["tdt"].ToString() + "    " + nsdr["tduration"].ToString() + "    " + nsdr["tpipe"].ToString() + "    " + nsdr["previous_data"].ToString());
                }

            }
            catch(Exception e)
            {
                Console.WriteLine("OOPs, something went wrong. " + e);
            }
            finally
            {
                con.Close();
            }
        }
    }
}
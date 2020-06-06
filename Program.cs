using System;
using System.Data.SqlClient;
using System.Xml;
using System.Xml.Linq;

namespace ConsoleApp1
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            string str = @"Data Source=DESKTOP-MAMTNTM\SQLEXPRESS;Initial Catalog=research;Integrated Security=True";
            SqlConnection con = new SqlConnection(str);

            Console.WriteLine("Adding Users");
            AddUsers(con);
            Console.WriteLine("Adding Posts");
            AddPosts(con);
            Console.WriteLine("Adding Post History");
            AddPostHistory(con);
        }

        private static void AddUsers(SqlConnection con)
        {
            int counter = 0;
            con.Open();
            string input = @"B:\Research\Users.xml";
            using (XmlReader reader = XmlReader.Create(input))
            {
                reader.MoveToContent(); // will not advance reader if already on a content node; if successful, ReadState is Interactive
                reader.Read();          // this is needed, even with MoveToContent and ReadState.Interactive
                while (!reader.EOF && reader.ReadState == ReadState.Interactive)
                {
                    counter += 1;

                    if (counter % 1000000 == 0)
                        Console.WriteLine($"Completed {counter} lines");

                    if (reader.NodeType == XmlNodeType.Element && reader.Name.Equals("row"))
                    {
                        // this advances the reader...so it's either XNode.ReadFrom() or reader.Read(), 
                        //but not both
                        XElement matchedElement = XNode.ReadFrom(reader) as XElement;
                        if (matchedElement != null)
                        {
                            using (SqlCommand cmd = con.CreateCommand())
                            {
                                var rep = matchedElement.Attribute("Reputation");
                                var id = matchedElement.Attribute("Id");
                                if (id != null && rep != null)
                                {
                                    cmd.CommandText = "Insert INTO dbo.users (xmlUserID, repuation) " +
                                        "VALUES (@xmlID,@xmlRep);";
                                    cmd.Parameters.AddWithValue("@xmlID", (Int64)id);
                                    cmd.Parameters.AddWithValue("@xmlRep", (Int64)rep);
                                    cmd.ExecuteNonQuery();
                                }
                            }
                        }
                    }
                    else
                        reader.Read();
                }
            }
            con.Close();
        }

        private static void AddPosts(SqlConnection con)
        {
            int counter = 0;
            con.Open();
            string input = @"B:\Research\Posts.xml";
            using (XmlReader reader = XmlReader.Create(input))
            {
                reader.MoveToContent(); // will not advance reader if already on a content node; if successful, ReadState is Interactive
                reader.Read();          // this is needed, even with MoveToContent and ReadState.Interactive
                while (!reader.EOF && reader.ReadState == ReadState.Interactive)
                {
                    counter += 1;

                    if (counter % 1000000 == 0)
                        Console.WriteLine($"Completed {counter} lines");
                    
                    if (reader.NodeType == XmlNodeType.Element && reader.Name.Equals("row"))
                    {
                        // this advances the reader...so it's either XNode.ReadFrom() or reader.Read(), but not both
                        XElement matchedElement = XNode.ReadFrom(reader) as XElement;
                        if (matchedElement != null)
                        {
                            var type = matchedElement.Attribute("PostTypeId");

                            if ((Int64)type == 1 && matchedElement.Attribute("ClosedDate") != null)
                            {
                                using (SqlCommand cmd = con.CreateCommand())
                                {
                                    var id = matchedElement.Attribute("Id");
                                    var owner = matchedElement.Attribute("OwnerUserId");
                                    if (id != null)
                                    {
                                        cmd.CommandText = "Insert INTO dbo.posts (xmlPostID, PostTypeID, OwnerUserID) VALUES (@xmlID,@xmlPostType, @xmlOwnerID);";
                                        cmd.Parameters.AddWithValue("@xmlID", (Int64)id);
                                        cmd.Parameters.AddWithValue("@xmlPostType", (Int64)type);
                                        if (owner != null)
                                        {
                                            cmd.Parameters.AddWithValue("@xmlOwnerID", (Int64)owner);
                                        }
                                        else
                                        {
                                            cmd.Parameters.AddWithValue("@xmlOwnerID", -1);
                                        }
                                        cmd.ExecuteNonQuery();
                                    }
                                }
                            }
                        }
                    }
                    else
                        reader.Read();
                }
            }
            con.Close();
        }

        private static void AddPostHistory(SqlConnection con)
        {
            int counter = 0;
            con.Open();
            string input = @"B:\Research\PostHistory.xml";
            using (XmlReader reader = XmlReader.Create(input))
            {
                reader.MoveToContent(); // will not advance reader if already on a content node; if successful, ReadState is Interactive
                reader.Read();          // this is needed, even with MoveToContent and ReadState.Interactive
                while (!reader.EOF && reader.ReadState == ReadState.Interactive)
                {
                    counter += 1;

                    if (counter % 1000000 == 0)
                        Console.WriteLine($"Completed {counter} lines");
                    
                    if (reader.NodeType == XmlNodeType.Element && reader.Name.Equals("row"))
                    {
                        // this advances the reader...so it's either XNode.ReadFrom() or reader.Read(), but not both
                        XElement matchedElement = XNode.ReadFrom(reader) as XElement;
                        if (matchedElement != null)
                        {
                            var type = matchedElement.Attribute("PostHistoryTypeId");

                            if ((Int64)type == 10)
                            {
                                using (SqlCommand cmd = con.CreateCommand())
                                {
                                    var id = matchedElement.Attribute("Id");
                                    var postID = matchedElement.Attribute("PostId");
                                    string comment = matchedElement.Attribute("Comment").Value.ToString();

                                    if (comment.Length > 20)
                                    {
                                        comment = comment.Substring(0, 20);
                                    }

                                    if (id != null && comment != null && postID != null)
                                    {
                                        cmd.CommandText = "Insert INTO dbo.postHistory (xmlPostHisID, xmlPostID, postHisTypeID, comment) VALUES (@xmlID, @xmlPostId, @postType, @comment);";
                                        cmd.Parameters.AddWithValue("@xmlID", (Int64)id);
                                        cmd.Parameters.AddWithValue("@xmlPostId", (Int64)postID);
                                        cmd.Parameters.AddWithValue("@postType", (Int64)type);
                                        cmd.Parameters.AddWithValue("@comment", comment);

                                        cmd.ExecuteNonQuery();
                                    }
                                }
                            }
                        }
                    }
                    else
                        reader.Read();
                }
            }
            con.Close();
        }
    }
}

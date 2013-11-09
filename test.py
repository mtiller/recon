from nido import Wall

with open("sample.nco", "w+") as fp:
    # Create the wall object with a file-like object to write to
    wall = Wall(fp)
    # Walls can contain tables, here is how we define one
    t = wall.add_table("time", "x", "y");
    # Tables can also have aliases, here is how we define a few
    t.add_alias("a", of="x", scale=1.0, offset=1.0);
    t.add_alias("b", of="y", scale=-1.0, offset=0.0);
    # Walls can also have objects.
    obj1 = wall.add_object("obj1");
    obj2 = wall.add_object("obj2");

    # Note, so far we have not specified the values of anything.
    # Once we "define" the wall, we can't change the structure,
    # but we can add rows to tables and fields to objects.
    wall.defined();

    t.add_row({"time": 0.0, "x": 1.0, "y": 2.0})
    wall.flush()  # We can write data out at any time
    t.add_row({"time": 1.0, "x": 0.0, "y": 3.0})
    wall.flush()
    t.add_row({"time": 2.0, "x": -1.0, "y": 3.0})
    wall.flush()

    obj1.add_field("name": "Mike");
    obj2.add_field("name": "Pete");

    wall.flush();

    obj1.add_field("nationality": "American");
    obj2.add_field("nationality": "UKLander");

    wall.flush();

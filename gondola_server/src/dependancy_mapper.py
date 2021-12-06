import re


class Object_Depend():
    """
    sort out the list
    """

    def __init__(self, dependancy_order):

        self.dependancy_order = dependancy_order

    def order(self):

        dependancy_order = self.dependancy_order

        schema_objects = []
        sequene_objects = []
        all_other_objects = []

        ordered_objects = []

        depend_list = []


        for x in dependancy_order:

            if x['db_object_type'] == 'schema':
                schema_objects.append(x)

            elif x['db_object_type'] == 'sequences':
                sequene_objects.append(x)

            else:
                all_other_objects.append(x)

        # The schema/sequences have been split out now
        # can just focus on rearranging all_other_objects

        for x in all_other_objects:
            for y in all_other_objects:
                parent_loop_obj_name = x['schema_name'] + \
                    '.' + x['object_name']
                definition = y['change_ddl']
                if parent_loop_obj_name in definition:
                    if y not in depend_list:
                        depend_list.append(y)

        # Remove depend object from normal objects

        for element in depend_list:
            if element in all_other_objects:
                all_other_objects.remove(element)

        # Now to sort the depend list into the correct order to release with
        moved_to_list = []

        while depend_list:

            temp_list = []

            for x in depend_list:

                for y in depend_list:

                    parent_loop_obj_name = x['schema_name'] + \
                        '.' + x['object_name']
                    definition = y['change_ddl']

                    if parent_loop_obj_name in definition:
                        if y not in temp_list:
                            temp_list.append(y)

            # Temp List should have objects which still have a further dependency
            # Move List should have objects which don't have further dependencies
            # Depend List should empty over each while loop as they should be moved into Move list


            if len(temp_list) > 0:
                # remove temp list from depend list and add that to move list
                for z in temp_list:
                    depend_list.remove(z)

                print(len(depend_list))

                for a in depend_list:
                    if a not in moved_to_list:
                        moved_to_list.append(a)

                # Set depend list to temp list as those are the remaing objects with dependencies
                depend_list = temp_list

            # No more dependecies left so just move everything over to move list and empty out depend list
            if len(temp_list) == 0:
                for a in depend_list:
                    if a not in moved_to_list:
                        moved_to_list.append(a)

                for z in depend_list:
                    depend_list.remove(z)

                # Remove Records that don't need to be there

        ordered_objects = schema_objects + sequene_objects + \
            all_other_objects + moved_to_list

        # Remove database name from object using regular expression
        pattern = '([A-Za-z0-9_$]*)\.([A-Za-z0-9_$]*)\.([A-Za-z0-9_$]*)'
        replace = "\g<2>.\g<3>"

        for x in ordered_objects:
            x['change_ddl'] = re.sub(pattern, replace, x['change_ddl'])

        return ordered_objects

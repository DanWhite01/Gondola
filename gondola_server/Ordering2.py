import re
import sys


def convert_fqn(object_name):

    object_name_array = object_name.split(".")

    # if the name don't contain double quotes then they must be case insensitive, so convert to upper
    if "\"" not in object_name_array[0]:
        object_name_array[0] = object_name_array[0].upper()

    if "\"" not in object_name_array[1]:
        object_name_array[1] = object_name_array[1].upper()

    return object_name_array[0] + "." + object_name_array[1]


def add_dependencies(parent_object, found_dependencies, deps):
    # calculate the parent schema, assumption is that parent_object is always schema.object_name
    parent_schema = parent_object.split(".")[0]

    # loop through all of the objects that have been found for the parent
    for match in found_dependencies:
        # group 1 contains the found object name
        dep_object_name = match.group(1)

        # is the object a fqn? (identified by having a "." within it)
        # if not, add the parent's schema to it
        if "." not in dep_object_name:
            dep_object_name = parent_schema + "." + dep_object_name

        dep_object_name = convert_fqn(dep_object_name)

        # if the found object is part of this release then add the dependancy to deps
        # if dep_object_name in deps.keys() and dep_object_name != parent_object:
        deps[parent_object].add(dep_object_name)


def calculate_deployment_order(deps):

    # now figure out the ordering
    deployment_order = []
    iteration_count = 0

    # loop while there are objects in the deps list
    while len(deps.keys()) > 0:
        iteration_count += 1
        print(f"Iteration {iteration_count}")
        # find keys where the values list is zero
        this_iter_deploys = []
        for parent_key, dep_list in deps.items():
            if len(dep_list) == 0:
                # add parent to this_iter_deploys
                print(f"Adding {parent_key} to deployment")
                this_iter_deploys.append(parent_key)

        # if no objects have been identified to deploy in this iteration we must have an infinite loop
        if len(this_iter_deploys) == 0:
            print("Infinite loop found")
            print(deps)
            raise Exception

        # for all objects in this iteration that have been marked for deploy
        # remove from deps
        # remove all references
        # add to the deployment order list
        for obj in this_iter_deploys:
            print(f"Handling {obj}")
            deps.pop(obj)
            for parent_key, dep_list in deps.items():
                print(f"removing {obj} from {parent_key} {dep_list}")
                dep_list.discard(obj)

        deployment_order = deployment_order + this_iter_deploys

        print(f"Dependencies now look like this {deps}")

    return deployment_order


def main():

    object_to_filename_mapping = dict()
    deps = dict()

    # loop over the files provided on the command line
    for arg in sys.argv[1:]:

        with open(arg, 'r') as file:
            ddl = file.read()

        # find object name in the ddl script
        # the regex returns 2 groups, the first group is the OBJECT TYPE and the second group is the OBJECT NAME
        object_name_regex = r"^create\s*(?:or\s*replace)?\s*(?:transient|secure)?\s*(?:recursive)?\s*(network\s*policy|resource\s*monitor|share|role|user|warehouse|schema|database|table|external\s*table|view|materialized\s*view|masking\s*policy|row\s*access\s*policy|sequence|tag|file\s*format|pipe|stream|task|function|external\s*function|procedure)\s*(?:if\s*not\s*exists)?\s*((?:\"?\w*\"?\.)*(?:\"?\w*\"?))"
        match = re.search(object_name_regex, ddl, re.MULTILINE | re.IGNORECASE)
        object_name = match.group(2)

        # if "." is not in name then not fully qualified so calc schema name
        if "." not in object_name:
            schema_regex = r"^use\s*schema\s*(\"?\w*\"?)"
            match = re.search(schema_regex, ddl, re.MULTILINE | re.IGNORECASE)
            schema_name = match.group(1)
            object_name = schema_name + "." + object_name

        #  sort out the quotes in the object_name
        object_name = convert_fqn(object_name)

        # add to object_to_filename_mapping and deps
        object_to_filename_mapping.update({object_name: arg})
        deps[object_name] = set()

        # find all tables/views in ddl
        table_regex = r"(?:into|from|join)(?![\(\s]*select)(?:[\s\(]*)((?:\"?\w+\"?\.)*(?:\"?\w+\"?))"
        matches = re.finditer(table_regex, ddl, re.MULTILINE | re.IGNORECASE)
        add_dependencies(object_name, matches, deps)

        # find all procs in ddl - identified by schema.proc(....) or proc(....)
        proc_regex = r"((?:\"?\w+\"?\.)*(?:\"?\w+\"?(?=\s*\(.*\))))"
        matches = re.finditer(proc_regex, ddl, re.MULTILINE | re.IGNORECASE)
        add_dependencies(object_name, matches, deps)

    # remove any identified dependencies that are not part of this release
    deployable_object_list = set(deps.keys())
    for x in deps.values():
        x.intersection_update(deployable_object_list)

    deployment_order = calculate_deployment_order(deps)
    print(f"Final deployment order is {deployment_order}")


if __name__ == "__main__":
    main()

def remove_string_elements(src_list):

    new_list = []
    for element in src_list:

        # Ignore this element if it is a string
        if isinstance(element, str):
            continue

        # Add each element to the new list
        new_list.append(element)

    return new_list
  
  
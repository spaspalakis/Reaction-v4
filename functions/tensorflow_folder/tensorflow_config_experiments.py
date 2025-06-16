# Experiments over tensorflow config files
import sys
import os
import copy
import tensorflow as tf
from functions.tensorflow_folder import file_tools
# sys.path.append('/home/gorfanidis/models/research/')


# from research.object_detection.utils import config_util


# def multi_getattr(obj, attr, **kw):
#     attributes = attr.split(".")
#     for i in attributes:
#         try:
#             obj = getattr(obj, i)
#             if callable(obj):
#                 obj = obj()
#         except AttributeError:
#             if kw.has_key('default'):
#                 return kw['default']
#             else:
#                 raise
#     return obj

def write_parameters_file(config_dict, parameters_file, override=True):
    # previous interactive version (slower)
    # accepted = True
    # if os.path.exists(parameters_file):
    #     accepted = False
    #     affirmative = ['Y', 'Yes', 'Yeah']
    #     affirmative += [x.lower() for x in affirmative]
    #     negative = ['N', 'No', 'Nope']
    #     negative += [x.lower() for x in negative]
    #     while not accepted:
    #         text = input("Parameters file {} already exists in folder. Override? [Y/N] ".format(parameters_file))
    #         if text in affirmative:
    #             accepted = True
    #             print('Overriding parameters file ...')
    #             break
    #         elif text in negative:
    #             print('Aborting ...')
    #             break
    # if accepted:
    if not os.path.exists(os.path.dirname(parameters_file)):
        os.makedirs(os.path.dirname(parameters_file))
    if not override and os.path.exists(parameters_file):
        parameters_file = file_tools.find_available_name(parameters_file, ext='.txt', suffix='')
        # raise ValueError('Parameters file {} already exists. Aborting...'.format(parameters_file))
    with open(parameters_file, 'w') as f:
        for key in sorted(config_dict):
            if key == 'input_path':
                total_size = 0
                # partial_size = sum(1 for _ in tf.python_io.tf_record_iterator(element))
                if isinstance(config_dict[key], list):
                    if len(config_dict[key]) > 1:
                        tfrecord_size = sum(1 for _ in tf.python_io.tf_record_iterator(config_dict[key][0][1:-2]))
                        total_size += tfrecord_size
                        f.write('{}: {} - {} samples\n'.format(key, config_dict[key][0][1:-2], tfrecord_size))
                        for i in range(1, len(config_dict[key])-1):
                            tfrecord_size = sum(1 for _ in tf.python_io.tf_record_iterator(config_dict[key][i][1:-2]))
                            f.write('{} {} - {} samples\n'.format(' ' * len(key), config_dict[key][i][1:-2],
                                                                  tfrecord_size))
                            total_size += tfrecord_size
                        # last line with no comma
                        tfrecord_size = sum(1 for _ in tf.python_io.tf_record_iterator(config_dict[key][-1][1:-1]))
                        f.write('{} {} - {} samples\n'.format(' ' * len(key), config_dict[key][-1][1:-1],
                                                              tfrecord_size))
                        total_size += tfrecord_size
                        f.write('Dataset size: {} samples\n'.format(total_size))
                    else:
                        # just a single input path was provided inside a list though
                        total_size = sum(1 for _ in tf.python_io.tf_record_iterator(config_dict[key][0][1:-1]))
                        f.write('{} {} - {} samples\n'.format(' ' * len(key), config_dict[key][0][1:-1], total_size))
                        f.write('Dataset size: {} samples\n'.format(total_size))
                else:
                    # just a single input path provided in str
                    total_size = sum(1 for _ in tf.python_io.tf_record_iterator(config_dict[key][1:-1]))
                    f.write('{} {} - {} samples\n'.format(' ' * len(key), config_dict[key][1:-1], total_size))
                    f.write('Dataset size: {} samples\n'.format(total_size))
            elif isinstance(config_dict[key], list):
                f.write('{}: {}\n'.format(key, config_dict[key][0]))
                for i in range(1, len(config_dict[key])):
                    f.write('{} {}\n'.format(' '*len(key), config_dict[key][i]))

            else:
                f.write('{}: {}\n'.format(key, config_dict[key]))


def config_parser(config_file, useful_values={}, verbose=False):
    """
    This function parses the tensorlfow config file to create a dict containing the most useful parameters
    :param config_file:
    :param prefined_values: A dict containing all parameters considered useful. If provided then it will be used
    instead of the build-in one. The value represents the depth of enclosed parameters each parameter annotation
    should include. Example: 'num_classes': 0 needs a single parameter to be clear. 'min_dimension': 1 needs 1 more
    so the output would be something like: image_resizer: min_dimension: 400 instead of min_dimension: 400 to make
    clearer what this parameter represent. It's an arbitrary choice actually.
    :param verbose: This is for debugging purposes only
    :return:
    """
    if not useful_values:
        useful_values = {'num_classes': 0, 'min_dimension': 1, 'max_dimension': 1, 'batch_size': 0,
                         'initial_learning_rate': 3, 'num_steps': 0, 'fine_tune_checkpoint': 0,
                         'input_path': 0, 'type': 2, 'from_detection_checkpoint': 0, 'data_augmentation_options': 0,
                         'label_map_path': 0, 'shuffle': 0, 'width': 1, 'height': 1}
    # else:
    #     useful_values = prefined_values
    sectors = []
    open_sector = False
    open_quotes = False
    config_dict = {}
    waiting_value = False
    key = None
    ignore_next = False
    buffer = []
    useful_value = ''
    with open(config_file, 'r') as f:
        j = 1
        for line in f:
            line = line.strip()
            if verbose:
                print('line ({}): {}'.format(j, line))
            # if j == 122 or j == 126 or j == 127 or j == 129:
            #     print('Gotcha')
            j += 1
            if not line or line.startswith('#'):
                continue
            items = line.split(' ')
            for item in items:
                if verbose:
                    print('item: {}'.format(item))
                # if item == 'data_augmentation_options':
                #     print('Gotcha')
                if item == '#':  # found a comment. Ignore all items after this in line
                    break
                if not item:
                    continue
                elif open_quotes:
                    if item.endswith('"') or item.endswith('\'') or item.endswith('",') or item.endswith('\',') or \
                            item.endswith('"]') or item.endswith('\']'):
                        item = '{} {}'.format(previous_item, item)
                        open_quotes = False
                        if item.endswith(']'):  # this means a list is closed so do the proper things to do
                            buffer.append(item[:-1])
                            if key in useful_values:
                                key_prefix = ''
                                for i in range(useful_values[key], 0, -1):
                                    # print(i)
                                    key_prefix += '{}: '.format(sectors[-1 - i])
                                config_dict['{}{}'.format(key_prefix, key)] = copy.deepcopy(
                                    buffer)  # int(item) if item.isdigit() else float(item)
                            # if not open_sector:
                            waiting_value = False
                            ignore_next = False
                            open_sector = False
                        elif open_sector:
                            # this means an item is parsed and the buffer is still open, so, ...
                            buffer.append(item)
                        elif waiting_value:
                            # writing the value to dict
                            # if key == 'initial_learning_rate':
                            #     print('Gotcha')
                            key_prefix = ''
                            for i in range(useful_values[key], 0, -1):
                                # print(i)
                                key_prefix += '{}: '.format(sectors[-1 - i])
                            config_dict[
                                '{}{}'.format(key_prefix, key)] = item  # int(item) if item.isdigit() else float(item)
                            # if not open_sector:
                            waiting_value = False
                            ignore_next = False

                    else:
                        previous_item = '{} {}'.format(previous_item, item)
                elif item == '{':
                    # this means that the item ended with : but since the current item is not a number the previous
                    # item is in fact a sector
                    if ignore_next:
                        sectors.append(key)
                    ignore_next = False
                    # open_sector = True
                elif item == '}':
                    # open_sector = False
                    if verbose:
                        print('Before sectors: {}'.format(sectors))
                    if useful_value:
                        key_prefix = ''
                        for i in range(sectors.index(useful_value), len(sectors)):
                            key_prefix += '{}: '.format(sectors[i])
                        if key_prefix.strip().endswith(':'):
                            key_prefix = key_prefix[:-2]
                        else:
                            key_prefix = key_prefix.strip()
                        config_dict[key_prefix] = ''
                        useful_value = ''
                    del sectors[-1]
                    if verbose:
                        print('After sectors: {}'.format(sectors))
                    ignore_next = False
                elif item.startswith('['):
                    if item.endswith(']'):  # this means a list with just a single element is given here
                        buffer = [item[1:-1]]
                        if key in useful_values:
                            key_prefix = ''
                            for i in range(useful_values[key], 0, -1):
                                # print(i)
                                key_prefix += '{}: '.format(sectors[-1 - i])
                            config_dict['{}{}'.format(key_prefix, key)] = copy.deepcopy(buffer)
                        waiting_value = False
                        ignore_next = False
                        open_sector = False
                    else:
                        open_sector = True
                        buffer = [item[1:]]
                        ignore_next = False
                elif (item.startswith('"') or item.startswith('\'')) and not (item.endswith('"') or item.endswith('\'')
                                                                              or item.endswith('",') or
                                                                              item.endswith('\',') or
                                                                              item.endswith('"]') or
                                                                              item.endswith('\']')):
                    open_quotes = True
                    previous_item = item
                elif item.endswith(']'):
                    buffer.append(item[:-1])
                    if key in useful_values:
                        key_prefix = ''
                        for i in range(useful_values[key], 0, -1):
                            # print(i)
                            key_prefix += '{}: '.format(sectors[-1 - i])
                        config_dict['{}{}'.format(key_prefix, key)] = copy.deepcopy(buffer)
                        # int(item) if item.isdigit() else float(item)
                    # if not open_sector:
                    waiting_value = False
                    ignore_next = False
                    open_sector = False
                elif open_sector:
                    buffer.append(item)
                elif waiting_value:  # writing the value to dict
                    # if key == 'initial_learning_rate':
                    #     print('Gotcha')
                    key_prefix = ''
                    for i in range(useful_values[key], 0, -1):
                        # print(i)
                        key_prefix += '{}: '.format(sectors[-1 - i])
                    config_dict['{}{}'.format(key_prefix, key)] = item  # int(item) if item.isdigit() else float(item)
                    # if not open_sector:
                    waiting_value = False
                    ignore_next = False
                elif item.endswith(':'):
                    if sectors:  # this means an inner sector was found and thus a value is expected afterwards
                        if item[:-1] in useful_values:
                            # config_dict[item[:-1]] = {}
                            waiting_value = True
                        key = item[:-1]
                        ignore_next = True
                    else:  # this means a root sector was found
                        sectors.append(item[:-1])
                elif not ignore_next:  # item != '{' and item != '}':
                    if item in useful_values:
                        useful_value = item
                        # # this assumes no child of this sector will be written. If not it won't work as expected
                        # key = item
                    # else:
                    #     useful_value = ''
                    sectors.append(item)
                    ignore_next = False
                else:
                    ignore_next = False
                if verbose:
                    print('item: {}, Sectors: {}, waiting_value={}, ignore_next={}, '
                          'open_sector={}'.format(item, sectors, waiting_value, ignore_next, open_sector))
                    print('config_dict={}'.format(config_dict))

        if verbose:
            print(config_dict)
        return config_dict


def main():
    # ====== Create the config dict
    # config_path = '/home/gorfanidis/Datasets/tfrecords/config/faster_rcnn_resnet101_roborder2.config'
    # config_dict = config_parser(config_path)

    # ========== write the parameters file ========
    config_path = '/home/gorfanidis/Datasets/tfrecords/config/faster_rcnn_resnet101_roborder2.config'
    parameters_file = '/home/gorfanidis/snapshots/parameters.txt'
    config_dict = config_parser(config_path)

    write_parameters_file(config_dict, parameters_file)


if __name__ == '__main__':
    main()
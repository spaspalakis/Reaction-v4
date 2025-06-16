import os
import re
from glob import glob
from math import floor
# from display_tools import print2
# try:
#     import xml_edit_tools
# except ImportError:
#     import dataset_tools2.xml_edit_tools as xml_edit_tools
# import raw_bounding_boxes_to_xml

import shutil


def find_available_name(filename_old, ext='.png', suffix=''):
    """
    A function to search for a non existing name with extenstion ext and using suffix (before the addition of the
    extension)
    :param filename_old:
    :param ext:
    :param suffix:
    :return: the new name
    """
    if ext and not ext.startswith('.'):
        ext = '.' + ext
    if suffix != '':
        suffix = '_' + suffix
    folder = os.path.dirname(filename_old)
    base_name = os.path.splitext(os.path.basename(filename_old))[0]
    output_name = base_name + suffix + ext
    folder = os.path.join(folder, "")

    i = 1
    while True:
        if not os.path.exists(folder + output_name):
            return folder + output_name
            break
        i += 1
        output_name = base_name + suffix + '_{}'.format(str(i)) + ext
    # print('The new filename is {}'.format(folder + output_name))


# def get_suffix(model_name, sep='_coco', sep2='checkpoints'):
#     if not sep in model_name:
#         return model_name
#         # raise ValueError('Model has not been trained in COCO dataset. Aborting ...')
#     suffix = model_name.split(sep)[0]
#     if sep2 in suffix:
#         suffix = suffix.split(sep2)[1][1:]
#     return suffix
#
#
# def get_parallel_folder(init_folder, new_folder):
#     if new_folder == '' or new_folder is None:
#         return os.path.join(init_folder, '')
#     if init_folder.endswith('/'):
#         base_folder = init_folder.rsplit('/', 2)[0]
#     elif init_folder.endswith('\\'):
#         base_folder = init_folder.rsplit('/', 2)[0]
#     else:
#         base_folder = init_folder.rsplit('/', 1)[0]
#     new_folder = os.path.join(base_folder, new_folder, '')
#     return new_folder
#
#
def get_list_files(folder_path, pattern='*', relative=True, extension=True):
    """
    Returns the list of files inside a folder
    :param folder_path:
    :param pattern:
    :param relative:
    :param extension:
    :return:
    """

    if not os.path.isdir(folder_path):
        if relative:
            if extension:
                return [os.path.basename(folder_path)]
            else:
                return [os.path.splitext(os.path.basename(folder_path))[0]]
        else:
            if extension:
                return [folder_path]
            else:
                return [os.path.splitext(folder_path)[0]]

    if relative:
        if extension:
            files_list = [os.path.relpath(x, folder_path) for x in glob(os.path.join(folder_path, pattern))]
        else:
            files_list = [os.path.splitext(os.path.relpath(x, folder_path))[0]
                          for x in glob(os.path.join(folder_path, pattern))]
    else:
        if extension:
            files_list = glob(os.path.join(folder_path, pattern))
        else:
            files_list = [os.path.splitext(x)[0] for x in glob(os.path.join(folder_path, pattern))]
    return files_list
#
#
# def get_list_folders(root_folder, relative=True):
#     if relative:
#         folder_list = [x for x in os.listdir(root_folder) if os.path.isdir(os.path.join(root_folder, x))]
#     else:
#         folder_list = [x for x in glob(os.path.join(root_folder, '*')) if os.path.isdir(x)]
#     return folder_list
#
#
# def create_file_subset(image_path, dest_rel_path, step, pattern='*.jpg', move=False, prefix=''):
#     """
#     Just a function to copy selected images from a folder to different one (normally a subfolder) with a given step
#     so as to create an evenly sampled subspace of the first image set
#     :param image_path: The folder the images are located
#     :param dest_rel_path: the path to the new folder (rel or absolute)
#     :param step: the given step
#     :param pattern: which files to select
#     :param move: whethere to move the files or not
#     :param prefix: if a prefix should be added to the files in the subset
#     :return:
#     """
#     image_list = get_list_files(image_path, pattern=pattern, relative=True)
#     dest_full_path = os.path.join(image_path, dest_rel_path)
#     if not os.path.exists(dest_full_path):
#         os.makedirs(dest_full_path)
#     if prefix == '-1':
#         prefix = os.path.basename(os.path.dirname(os.path.join(image_path, "")))
#     if len(image_list) == 0:
#         raise ValueError('{} did not contain any element of type {}'.format(image_path, pattern))
#     # get image pattern
#     if prefix and not prefix.endswith('_'):
#         prefix = '{}_'.format(prefix)
#     if '_' in image_list[-1]:
#         name_prefix, main_name = image_list[-1].rsplit('_', 1)
#         name_prefix = '{}_'.format(name_prefix)
#     else:
#         main_name = image_list[-1]
#         name_prefix = ''
#     num_name, ext = main_name.split('.')
#     length = len(num_name)
#     final = int(num_name)
#     n = int(floor(final / step))
#     print('{} out of {} files will be {} to {}...'.format(n, len(image_list), 'moved' if move else 'copied',
#                                                            dest_rel_path))
#     for i in range(0, n+1):
#         name = '{}{:0{}}.{}'.format(name_prefix, i*step, length, ext)
#         full_name = os.path.join(dest_full_path, '{}{}'.format(prefix, name))
#         if os.path.exists(full_name):
#             raise ValueError('{} file already exists. Aborting'.format(full_name))
#         if move:
#             shutil.move(os.path.join(image_path, image_list[i*step]), full_name)
#         else:
#             shutil.copy2(os.path.join(image_path, image_list[i*step]), full_name)
#     print(' OK')
#
#
# def copy_images_subfolders(image_base_folder, ext='*.png', excluded_exts=[], move_single_folder=False):
#     image_list = get_list_files(image_base_folder, ext, relative=False)
#     if move_single_folder:
#         dest_subfolder = 'inferred'
#         if os.path.exists(os.path.join(image_base_folder, dest_subfolder)):
#             dest_subfolder = find_available_name(os.path.join(image_base_folder, dest_subfolder), ext='')
#         # dest_subfolder = os.path.join(image_base_folder, dest_subfolder)
#         os.mkdir(os.path.join(image_base_folder, dest_subfolder))
#     i = 0
#     j = 0
#     for im in image_list:
#         if 'RCNN' in im:
#             subfolder = 'RCNN{}'.format(os.path.basename(im).rsplit('.')[0].rsplit('RCNN')[1])
#         else:
#             subfolder = 'SSD{}'.format(os.path.basename(im).rsplit('.')[0].rsplit('SSD')[1])
#         # if len(subfolder.split('_')) > 2:
#         #     base_subfolder = subfolder.split('_')[1]
#         # else:
#         #     base_subfolder = subfolder
#         base_subfolder = subfolder.split('_')[1]
#         if base_subfolder in excluded_exts:
#             continue
#         if not move_single_folder:
#             if not os.path.exists(os.path.join(os.path.dirname(im), subfolder)):
#                 os.mkdir(os.path.join(os.path.dirname(im), subfolder))
#                 j += 1
#             dest_subfolder = os.path.join(os.path.dirname(im), subfolder)
#         shutil.move(im, os.path.join(os.path.dirname(im), dest_subfolder, os.path.basename(im)))
#         i += 1
#     if move_single_folder:
#         print('{}/{} images were moved in a single subfolder named: {}.'.format(i, len(image_list), dest_subfolder))
#     else:
#         print('{}/{} images were moved in subfolders of which {} were newly created.'.format(i, len(image_list), j))
#
#
# def create_image_xml_structure(base_path, pattern='*.jpg', anno_subfolder='annotations', delete_orphan_images=True,
#                                check_xml_exist=True):
#     """
#     This function copies all xml files in folder to a subfolder and also moves/deletes all jpg images that do not
#     have a corresponding xml file fin folder
#     :param base_path:
#     :param pattern:
#     :param anno_subfolder:
#     :param delete_orphan_images:
#     :param check_xml_exist:
#     :return:
#     """
#     orphaned_folder = 'orphaned'
#     xml_list = get_list_files(base_path, pattern='*.xml', relative=True)
#     if not delete_orphan_images:
#         if not os.path.exists(os.path.join(base_path, orphaned_folder)):
#             os.mkdir(os.path.join(base_path, orphaned_folder))
#     if check_xml_exist and len(xml_list) == 0:
#         raise ValueError('No xml files found in folder {}'.format(base_path))
#     image_list = get_list_files(base_path, pattern, relative=True)
#     if not os.path.exists(os.path.join(base_path, anno_subfolder)):
#         os.makedirs(os.path.join(base_path, anno_subfolder))
#     deleted = 0
#     for i, f in enumerate(image_list):
#         xml_name = '{}.xml'.format(f.rsplit('.', 1)[0])
#         if xml_name in xml_list:
#             shutil.move(os.path.join(base_path, xml_name), os.path.join(base_path, anno_subfolder, xml_name))
#         else:
#             if delete_orphan_images:
#                 os.remove(os.path.join(base_path, f))
#             else:
#                 shutil.move(os.path.join(base_path, f), os.path.join(base_path, orphaned_folder, f))
#             deleted += 1
#     if not os.listdir(os.path.join(base_path, orphaned_folder)):
#         os.rmdir(os.path.join(base_path, orphaned_folder))
#     if delete_orphan_images:
#         print('{} images were deleted. {} xml files were placed in {} subfolder'.format(deleted,
#                                                                                         len(image_list)-deleted,
#                                                                                         anno_subfolder))
#     else:
#         print('{} images were moved in {} folder. {} xml files were placed in {} subfolder'.format(deleted,
#                                                                                                    orphaned_folder,
#                                                                                                    len(image_list) -
#                                                                                                    deleted,
#                                                                                                    anno_subfolder))
#     if len(xml_list):
#         xml_edit_tools.correct_xml_name(os.path.join(base_path, anno_subfolder))
#         xml_edit_tools.change_and_write_image_location_folder(os.path.join(base_path, anno_subfolder), base_path)
#         print('All {} xml file locations were fixed'.format(len(xml_list)))
#
#
# def main():
#     # # copy sampled images to a subfolder
#     # for i in range(12, 24):
#     #     image_path = 'D:/Dataset/UAV123/data_seq/UAV123/person{}'.format(i)#'D:/Dataset/UAV123/anno2/boat2'#
#     #     dest_rel_path = 'subset'
#     #     step = 30
#     #     pattern = '*.jpg'
#     #     create_file_subset(image_path, dest_rel_path, step, pattern=pattern, move=False, prefix='-1')
#     #     # create the corresponding xml files
#     #     base_folder = 'D:/Dataset/UAV123/'
#     #     rel_anno_folder = 'anno/UAV123'
#     #     rel_image_folder = image_path.split('/', 3)[-1] #'data_seq/UAV123/car1'
#     #     # step = 30
#     #     raw_bounding_boxes_to_xml.create_xml(base_folder, rel_anno_folder, rel_image_folder, object_name='-1',
#     #                                          pattern='*.jpg', anno_output='anno2', prefix='-1', step=30)
#
#     # # now copy corresponding xml files
#     # anno_path = 'D:/Dataset/UAV123/anno2/truck4'
#     # anno_subfolder = 'annotations'
#     # dest_full = os.path.join(image_path, dest_rel_path, anno_subfolder)
#     # create_file_subset(anno_path, dest_full, step, pattern='*.xml', move=False, prefix='')
#
#     # create a structure suitable for object detection
#     # for i in range(1, 24):
#     #     base_path = 'D:/Dataset/UAV123/data_seq/UAV123/person{}/subset'.format(i)#'D:/Dataset/Video dataset/drone misc/Traffic Driving On A Motorway_frames'
#     #     create_image_xml_structure(base_path, pattern='*.jpg', anno_subfolder='annotations')
#
#     # base_path = '/home/gorfanidis/Datasets/web-images/size_experiments/train/original2' #'D:/Dataset/web-images/people misc/weapons'
#     # create_image_xml_structure(base_path, pattern='*.jpg', anno_subfolder='annotations', delete_orphan_images=False)
#
#     # =========== copy all inference files to subfolders depending on their prefix
#     # image_base_folder = '/home/gorfanidis/Datasets/web-images/size_experiments/test/original2/'
#     # copy_images_subfolders(image_base_folder, ext='*.png')
#
#     # =========== copy some inference files to subfolders depending on their prefix (if they are going to be
#     # copied at all)
#     image_base_folder = '/home/gorfanidis/Datasets/web-images/roborder/test'
#     excluded_exts = []  # ['036', '060', '061', '005']  # []  #
#     copy_images_subfolders(image_base_folder, ext='*.png', excluded_exts=excluded_exts, move_single_folder=True)
#
#
# if __name__ == '__main__':
#     main()
#

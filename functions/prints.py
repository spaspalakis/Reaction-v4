import time



def fps_in_a_sec(fps_values,processed_frame_list,processed_frames,OneSec_time):
            

    # FPS Calculation: After 1 second, print the FPS and reset counters
    elapsed_time = time.time() - OneSec_time
    if elapsed_time >= 1.0:
        current_fps = processed_frames / elapsed_time
        # print(f"\n[INFO] 1sec passed, FPS (Processed): {current_fps:.2f}")
        #print(f"[INFO] Processed frames: {processed_frame_list}")

        fps_values.append(current_fps)  # Store FPS value
        OneSec_time = time.time()  # Reset the timer
        processed_frames = 0  # Reset the frame counter
        processed_frame_list = []    
        
        
    return processed_frame_list,fps_values
    

def fps_every_50(total_processed_frames):
    if total_processed_frames % 50==0:
        end_time_50_frames = time.time()  # Get the current time
        elapsed_time_50_frames = end_time_50_frames - start_time_50_frames
                
        if elapsed_time_50_frames > 0:
            average_fps_50 = 50 / elapsed_time_50_frames  # Calculate average FPS for the last 50 frames
            print(f"\n[INFO] Avg FPS for the last 50 processed frames: {average_fps_50:.2f}")
        else:
            print("[INFO] No Avg frames proceesed in 50 frames")

        start_time_50_frames = time.time()  # Reset the start time for the next 50-frame batch
    
    return


def final_print(start_time_total,total_processed_frames,fps_values):
    # End of loop, calculate total elapsed time and average FPS
    end_time_total = time.time()  # End time to calculate total processing time
    total_elapsed_time = end_time_total - start_time_total

    if total_elapsed_time > 0:
        average_fps = total_processed_frames / total_elapsed_time
        print(f"\n[INFO] Total frames processed: {total_processed_frames}")
        # print(f"[INFO] Total elapsed time: {total_elapsed_time:.2f} seconds")
        print(f"[INFO] Average FPS: {average_fps:.2f}")
    else:
        print(f"\n[INFO] No time elapsed or no frames processed.")

    if fps_values:
        average_fps_per_second = sum(fps_values) / len(fps_values)
        print(f"\n[1sec] Avg frames proceesed in a sec: {average_fps_per_second:.2f}")
    else:
        print("\n[1sec] No Avg frames proceesed in a sec.")
    
    return
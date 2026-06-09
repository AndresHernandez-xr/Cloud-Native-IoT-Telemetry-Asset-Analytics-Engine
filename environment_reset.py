import os
import shutil

print("🧹 Step 1: Initiating complete environment wipe...")

# 1. Stop any background active stream contexts
for q in spark.streams.active: 
    q.stop()

# 2. Re-establish paths inside your open personal folder
user_email = spark.sql("SELECT current_user()").collect()[0][0]
base_dir = f"/Workspace/Users/{user_email}/sandbox_project_staged"

landing_path = f"{base_dir}/landing"
checkpoint_path = f"{base_dir}/checkpoints"
schema_tracking_path = f"{base_dir}/schema_tracking"

# 3. Physically delete old directory tracking files
if os.path.exists(landing_path): shutil.rmtree(landing_path)
if os.path.exists(checkpoint_path): shutil.rmtree(checkpoint_path)
if os.path.exists(schema_tracking_path): shutil.rmtree(schema_tracking_path)

# 4. Re-create pristine, completely empty tracking directories
os.makedirs(landing_path, exist_ok=True)
os.makedirs(checkpoint_path, exist_ok=True)
os.makedirs(schema_tracking_path, exist_ok=True)

print("✅ Success! All old system schemas and file histories have been purged.")
print("👉 Proceed to Step 2.")
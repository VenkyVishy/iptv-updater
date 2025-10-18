#!/bin/bash
cd "$(dirname "$0")"
while true; do
    echo "[$(date)] Running IPTV Updater..."
    python vengatesh_iptv_v23.py
    echo "Sleeping for 6 hours..."
    sleep 21600
done
cat > run_iptv.sh << 'EOF'
#!/bin/bash
cd "$(dirname "$0")"
while true; do
    echo "[$(date)] Running IPTV Updater..."
    python vengatesh_iptv_v23.py
    echo "Sleeping for 6 hours..."
    sleep 10
done


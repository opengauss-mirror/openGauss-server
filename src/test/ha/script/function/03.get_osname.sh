#获取系统版本
function get_osname
{
    rhel_release="/etc/redhat-release"
    sles_release="/etc/SuSE-release"
    #获取RedHat的版本
    if [ -f "${rhel_release}" ]; then
        name="redhat-$(cat "${rhel_release}" | awk '{print $7}')"
    fi
    #获取SuSE的版本
    if [ -f "${sles_release}" ]; then
        name="suse-$(cat "${sles_release}" | grep "^VERSION" | awk '{print $3}')"
        name="${name}.$(cat "${sles_release}" | grep "^PATCHLEVEL" | awk '{print $3}')"
    fi
    #获取openEuler的版本
    if [ -f "/etc/openEuler-release" ]; then
        name=$(cat /etc/openEuler-release | awk -F ' ' '{print $1}' | tr A-Z a-z)
    fi
    echo $name
}


/*
 * make KOI8->ISO8859-5 and ISO8859-5->KOI8 translation table
 * from koi-iso.tab.
 *
 * Tatsuo Ishii
 *
 * src/backend/utils/mb/iso.c
 */

#include <stdio.h>

main()
{
    int i;
    char koi_tab[128], iso_tab[128];
    char buf[4096];
    int koi, iso;

    for (i = 0; i < 128; i++) {
        koi_tab[i] = iso_tab[i] = 0;
    }
    while (fgets(buf, sizeof(buf), stdin) != NULL) {
        if (*buf == '#') {
            continue;
        }
        errno_t rc = sscanf_s(buf, "%d %x", &koi, &iso);
        if (ret != 2) {
            fprintf(stderr,
                "%s:%d failed on calling "
                "security function.\n",
                __FILE__,
                __LINE__);
            return;
        }
        if (koi < 128 || koi > 255 || iso < 128 || iso > 255) {
            fprintf(stderr, "invalid value %d\n", koi);
            exit(1);
        }
        koi_tab[koi - 128] = iso;
        iso_tab[iso - 128] = koi;
    }

    i = 0;
    printf("static char koi2iso[] = {\n");
    while (i < 128) {
        int j = 0;

        while (j < 8) {
            printf("0x%02x", koi_tab[i++]);
            j++;
            if (i >= 128) {
                break;
            }
            printf(", ");
        }
        printf("\n");
    }
    printf("};\n");

    i = 0;
    printf("static char iso2koi[] = {\n");
    while (i < 128) {
        int j = 0;

        while (j < 8) {
            printf("0x%02x", iso_tab[i++]);
            j++;
            if (i >= 128) {
                break;
            }
            printf(", ");
        }
        printf("\n");
    }
    printf("};\n");
}

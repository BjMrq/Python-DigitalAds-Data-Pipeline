import fetching_facebook_data as fetch
import cleanning_faceboo_data as clean
import load_data_bigquery as load
import delete_csv as delete
import send_finished_email as email

fetch.main()
clean.main()
load.main()
delete.main()
email.main()

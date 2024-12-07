FROM quay.io/astronomer/astro-runtime:12.5.0

USER root

ADD https://storage.googleapis.com/chrome-for-testing-public/131.0.6778.87/linux64/chrome-linux64.zip /tmp/chrome.zip
ADD https://storage.googleapis.com/chrome-for-testing-public/131.0.6778.87/linux64/chromedriver-linux64.zip /tmp/chrome-driver.zip

RUN apt-get update && \
apt-get install -y unzip && \
apt-get clean && \
unzip /tmp/chrome.zip -d /usr/local/bin/ && \
unzip /tmp/chrome-driver.zip -d /usr/local/bin/ && \
chmod 755 /usr/local/bin && \
rm /tmp/chrome*

USER astro
